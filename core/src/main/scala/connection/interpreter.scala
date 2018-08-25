package rabid
package connection

import java.net.InetSocketAddress
import java.nio.channels.AsynchronousChannelGroup

import scala.concurrent.ExecutionContext

import fs2.{Stream, Chunk, Pull}
import fs2.io.tcp
import fs2.interop.scodec.ByteVectorChunk
import scodec.{Encoder, Decoder, Attempt, DecodeResult, Err}
import scodec.bits.{BitVector}
import cats.~>
import cats.data.{EitherT, OptionT}
import cats.effect.IO
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

import channel.{ChannelConnection, Channel, ChannelInput}

object Interpreter
{
  def send(client: tcp.Socket[IO])(message: Message): ConnectionA.Effect[Unit] =
    Encoder.encode(message) match {
      case Attempt.Successful(bits) =>
        ConnectionA.Effect.eval(client.write(ByteVectorChunk(bits.toByteVector)))
      case Attempt.Failure(error) =>
        for {
          _ <- ConnectionA.Effect.error(s"couldn't encode message $message: $error")
          _ <- EitherT.leftT[ConnectionA.State, Unit](error)
        } yield ()
    }

  def startChannel(connection: ChannelConnection)
  (channel: Stream[IO, Input])
  : ConnectionA.State[Unit] =
    for {
      pool <- ConnectionA.State.inspect(_.pool)
      _ <- ConnectionA.State.eval(pool.enqueue1(channel))
      _ <- ConnectionA.State.modify(data => data.copy(channels = data.channels.updated(connection.number, connection)))
    } yield ()

  def startControlChannel
  : ConnectionA.State[Unit] =
    for {
      connection <- ConnectionA.State.inspect(_.channel0)
      _ <- startChannel(connection)(Channel.runControl(connection))
    } yield ()

  def smallestUnused(numbers: Iterable[Short]): Int =
    numbers.foldLeft(1)((z, a) => if (z < a) z else a + 1)

  def unusedChannelNumber: ConnectionA.State[Short] =
    for {
      numbers <- ConnectionA.State.inspect(_.channels.keys)
    } yield smallestUnused(numbers).toShort

  def consChannel(channel: Channel)
  (implicit ec: ExecutionContext)
  : ConnectionA.State[ChannelConnection] =
    for {
      number <- unusedChannelNumber
      connection <- ConnectionA.State.eval(ChannelConnection.cons(number.toShort, channel))
    } yield connection

  def channelConnection(number: Short): ConnectionA.Effect[ChannelConnection] =
    for {
      stored <- EitherT.liftF(ConnectionA.State.inspect(_.channels.get(number)))
      connection <- stored match {
        case Some(channel) => ConnectionA.Effect.pure(channel)
        case None =>
          ConnectionA.Effect.either(Left(Err.General(s"no such channel: $number", Nil)))
      }
    } yield connection

  def runInChannel(connection: ChannelConnection)(thunk: ChannelInput.Prog)
  : ConnectionA.State[Unit] =
    ConnectionA.State.eval(connection.channel.exchange.in.enqueue1(thunk))

  def runInControlChannel(thunk: ChannelInput.Internal)
  : ConnectionA.State[Unit] =
    for {
      _ <- ConnectionA.State.eval(log("running job in control channel"))
      connection <- ConnectionA.State.inspect(_.channel0)
      _ <- ConnectionA.State.eval(connection.channel.exchange.in.enqueue1(thunk))
    } yield ()

  def createChannel(channel: Channel)
  (implicit ec: ExecutionContext)
  : ConnectionA.State[Unit] =
    for {
      connection <- consChannel(channel)
      _ <- startChannel(connection)(Channel.run(connection))
    } yield ()

  def sendToChannel(header: FrameHeader, body: FrameBody)
  : ConnectionA.Effect[Unit] =
    for {
      connection <- channelConnection(header.channel)
      _ <- ConnectionA.Effect.eval(log(s"sending to channel ${connection.number}"))
      _ <- ConnectionA.Effect.eval(connection.receive.enqueue1(body.payload))
    } yield ()

  def notifyChannel(number: Short, input: ChannelInput)
  : ConnectionA.Effect[Unit] =
    for {
      connection <- channelConnection(number)
      _ <- ConnectionA.Effect.eval(connection.channel.exchange.in.enqueue1(input))
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
              Log.info[IO]("connection", s"decoded rabbit message $a").as(Some(a))
            case Attempt.Failure(err) =>
              Log.error[IO]("connection", s"rabbit chunk decoding failed for `$description`: $err | $bits").as(None)
          }
        case None =>
          IO.pure(None)
      }
    } yield output

  def receiveFrame(client: tcp.Socket[IO]): OptionT[IO, Input] =
    for {
      header <- OptionT(receiveAs[FrameHeader](client)("frame header")(7))
      body <- OptionT(receiveAs[FrameBody](client)("frame body")(header.size + 1)(FrameBody.codec(header.size)))
    } yield Input.ChannelReceive(header, body)

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

  def log(message: String): IO[Unit] =
    for {
      logger <- Slf4jLogger.fromName[IO]("connection")
      _ <- logger.info(message)
    } yield ()

  def nativeInterpreter(client: tcp.Socket[IO])
  (implicit ec: ExecutionContext)
  : ConnectionA ~> ConnectionA.Effect =
    new (ConnectionA ~> ConnectionA.Effect) {
      def apply[A](action: ConnectionA[A]): ConnectionA.Effect[A] = {
        action match {
          case ConnectionA.Send(message) =>
            send(client)(message)
          case ConnectionA.StartControlChannel =>
            EitherT.liftF(startControlChannel)
          case ConnectionA.RunInControlChannel(thunk) =>
            EitherT.liftF(runInControlChannel(thunk))
          case ConnectionA.ChannelReceive(header, body) =>
            sendToChannel(header, body)
          case ConnectionA.NotifyChannel(number, input) =>
            notifyChannel(number, input)
          case ConnectionA.Log(message) =>
            ConnectionA.Effect.eval(log(message))
          case ConnectionA.OpenChannel(channel) =>
            EitherT.liftF(createChannel(channel))
          case ConnectionA.ChannelOpened(_, _) =>
            EitherT.pure(())
        }
      }
    }

  def native(host: String, port: Int)
  (implicit ec: ExecutionContext, ag: AsynchronousChannelGroup)
  : Stream[IO, (Stream[IO, Input], ConnectionA ~> ConnectionA.Effect)] =
    for {
      client <- tcp.client[IO](new InetSocketAddress(host, port))
    } yield (listenRabbit(client), (nativeInterpreter(client)))
}
