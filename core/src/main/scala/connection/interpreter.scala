package rabid
package connection

import java.net.InetSocketAddress
import java.nio.channels.AsynchronousChannelGroup

import scala.concurrent.ExecutionContext

import fs2.{Stream, Chunk, Pull}
import fs2.io.tcp
import fs2.interop.scodec.ByteVectorChunk
import fs2.async.mutable.{Queue, Signal}
import scodec.{Encoder, Decoder, Attempt, DecodeResult, Err}
import scodec.bits.{BitVector}
import cats.~>
import cats.data.{EitherT, OptionT}
import cats.effect.IO
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

import channel.{ChannelConnection, Channel, ChannelProg, programs}

object Interpreter
{
  def send(client: tcp.Socket[IO])(message: Message): Action.Effect[Unit] =
    Encoder.encode(message) match {
      case Attempt.Successful(bits) =>
        Action.Effect.eval(client.write(ByteVectorChunk(bits.toByteVector)))
      case Attempt.Failure(error) =>
        for {
          _ <- Action.Effect.error(s"couldn't encode message $message: $error")
          _ <- EitherT.leftT[Action.State, Unit](error)
        } yield ()
    }

  def channelOutput(channels: Connection.ChannelPool)
  (implicit ec: ExecutionContext)
  : Stream[IO, Input] =
    channels.dequeue.join(10)

  def startChannel(connection: ChannelConnection)
  (implicit ec: ExecutionContext)
  : Action.State[Unit] =
    for {
      pool <- Action.State.inspect(_.pool)
      _ <- Action.State.eval(pool.enqueue1(Channel.run(connection)))
      _ <- Action.State.modify(data => data.copy(channels = data.channels.updated(connection.number, connection)))
    } yield ()

  def startControlChannel
  (implicit ec: ExecutionContext)
  : Action.State[Unit] =
    for {
      connection <- Action.State.inspect(_.channel0)
      _ <- startChannel(connection)
    } yield ()

  def smallestUnused(numbers: Iterable[Short]): Int =
    numbers.foldLeft(1)((z, a) => if (z < a) z else a + 1)

  def unusedChannelNumber: Action.State[Short] =
    for {
      numbers <- Action.State.inspect(_.channels.keys)
    } yield smallestUnused(numbers).toShort

  def consChannel(channel: Channel)
  (implicit ec: ExecutionContext)
  : Action.State[ChannelConnection] = {
    for {
      number <- unusedChannelNumber
      connected <- Action.State.inspect(_.connected)
      connection <- Action.State.eval(ChannelConnection.cons(number.toShort, channel, connected))
    } yield connection
  }

  def channelConnection(number: Short)
  : Action.Effect[ChannelConnection] =
    for {
      stored <- EitherT.liftF(Action.State.inspect(_.channels.get(number)))
      connection <- stored match {
        case Some(channel) => Action.Effect.pure(channel)
        case None =>
          Action.Effect.either(Left(Err.General(s"no such channel: $number", Nil)))
      }
    } yield connection

  def runInChannel(channel: ChannelConnection)(thunk: ChannelProg)
  : Action.State[Unit] =
    Action.State.eval(channel.progs.enqueue1(thunk))

  def runInControlChannel(thunk: ChannelProg)
  : Action.State[Unit] =
    for {
      _ <- Action.State.eval(log("running job in control channel"))
      channel <- Action.State.inspect(_.channel0)
      _ <- Action.State.eval(channel.progs.enqueue1(thunk))
    } yield ()

  def createChannel(channel: Channel)
  (implicit ec: ExecutionContext)
  : Action.State[Unit] =
    for {
      connection <- consChannel(channel)
      _ <- startChannel(connection)
      _ <- runInChannel(connection)(
        ChannelProg(s"create channel ${connection.number}", programs.createChannel(connection.number))
      )
    } yield ()

  def sendToChannel(header: FrameHeader, body: FrameBody)
  : Action.Effect[Unit] =
    for {
      connection <- channelConnection(header.channel)
      _ <- Action.Effect.eval(log(s"sending to channel ${connection.number}"))
      _ <- Action.Effect.eval(connection.receive.enqueue1(body.payload))
    } yield ()

  def receive(client: tcp.Socket[IO])(numBytes: Int): IO[Option[BitVector]] =
    client.read(numBytes)
      .map((a: Option[Chunk[Byte]]) => a.map(_.toVector).map(BitVector(_)))

  def receiveAs[A: Decoder](client: tcp.Socket[IO])(description: String)(numBytes: Int): IO[Option[A]] =
    for {
      response <- receive(client)(numBytes)
      output <- response match {
        case Some(bits) =>
          Decoder[A].decode(bits) match {
            case Attempt.Successful(DecodeResult(a, _)) =>
              Log.info("connection", s"decoded rabbit message $a").as(Some(a))
            case Attempt.Failure(err) =>
              Log.error("connection", s"rabbit chunk decoding failed for `$description`: $err | $bits").as(None)
          }
        case None =>
          IO.pure(None)
      }
    } yield output

  def listenChannels(pool: Connection.ChannelPool)
  (implicit ec: ExecutionContext)
  : Stream[IO, Input] =
    channelOutput(pool)

  def receiveFrame(client: tcp.Socket[IO]): OptionT[IO, Input] =
    for {
      header <- OptionT(receiveAs[FrameHeader](client)("frame header")(7))
      body <- OptionT(receiveAs[FrameBody](client)("frame body")(header.size + 1)(FrameBody.codec(header.size)))
    } yield Input.SendToChannel(header, body)

  def listenRabbitLoop(client: tcp.Socket[IO])(state: Unit): Pull[IO, Input, Option[Unit]] =
    for {
      response <- Pull.eval(receiveFrame(client).value)
      a <- response match {
        case Some(a) => Pull.output1(a) >> Pull.pure(Some(()))
        case None => Pull.pure(Some(()))
      }
    } yield a.map(_ => state)

  def listenRabbit(client: tcp.Socket[IO]): Stream[IO, Input] =
    Pull.loop(listenRabbitLoop(client))(()).stream

  def listen(client: tcp.Socket[IO], pool: Connection.ChannelPool, channels: Queue[IO, Input])
  (implicit ec: ExecutionContext)
  : Stream[IO, Input] =
    listenChannels(pool).merge(listenRabbit(client)).merge(channels.dequeue)

  def log(message: String): IO[Unit] =
    for {
      logger <- Slf4jLogger.fromName[IO]("connection")
      _ <- logger.info(message)
    } yield ()

  def nativeInterpreter(client: tcp.Socket[IO], listen: Queue[IO, Input])
  (implicit ec: ExecutionContext)
  : Action ~> Action.Effect =
    new (Action ~> Action.Effect) {
      def apply[A](action: Action[A]): Action.Effect[A] = {
        action match {
          case Action.Listen =>
            Action.Effect.eval(listen.dequeue1)
          case Action.Send(message) =>
            send(client)(message)
          case Action.StartControlChannel =>
            EitherT.liftF(startControlChannel)
          case Action.RunInControlChannel(thunk) =>
            EitherT.liftF(runInControlChannel(thunk))
          case Action.SendToChannel(header, body) =>
            sendToChannel(header, body)
          case Action.Log(message) =>
            Action.Effect.eval(log(message))
          case Action.CreateChannel(channel) =>
            EitherT.liftF(createChannel(channel))
          case Action.ChannelCreated(_, _) =>
            EitherT.pure(())
          case Action.SetConnected(state) =>
            for {
              connected <- EitherT.liftF(Action.State.inspect(_.connected))
              _ <- Action.Effect.eval(connected.set(state))
            } yield ()
        }
      }
    }

  def native(host: String, port: Int)
  (implicit ec: ExecutionContext, ag: AsynchronousChannelGroup)
  : Stream[IO, (
    Connection.ChannelPool,
    Stream[IO, Unit],
    Queue[IO, Input],
    Signal[IO, Boolean],
    Action ~> Action.Effect,
    IO[Unit],
    )] =
    for {
      client <- tcp.client[IO](new InetSocketAddress(host, port))
      pool <- Stream.eval(Queue.unbounded[IO, Stream[IO, Input]])
      queue <- Stream.eval(Queue.unbounded[IO, Input])
      input <- Stream.eval(Queue.unbounded[IO, Input])
      connected <- Stream.eval(Signal[IO, Boolean](false))
    } yield (
      pool,
      listen(client, pool, input).to(queue.enqueue),
      input,
      connected,
      nativeInterpreter(client, queue),
      client.close,
    )
}
