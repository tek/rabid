package rabid

import scala.concurrent.ExecutionContext

import fs2.Stream
import fs2.async.mutable.{Queue, Signal}
import cats.implicits._
import cats.effect.IO
import _root_.io.circe.{Encoder, Decoder}
import _root_.io.circe.syntax._
import _root_.io.circe.parser._

import connection.{Connection, Input, ConnectionConfig}
import channel.{Channel, ChannelA, ChannelInput, ChannelOutput, programs, ChannelMessage, ExchangeConf, QueueConf}

case class Message[A](data: A, deliveryTag: Long)

object Api
{
  def send(name: String, thunk: Channel.Prog)(channel: Channel): Stream[IO, Unit] =
    Stream.eval(channel.exchange.in.enqueue1(ChannelInput.Prog(name, thunk)))

  def declareExchange(conf: ExchangeConf): Channel => Stream[IO, Unit] =
    send(s"declare exchange `${conf.name}`", programs.declareExchange(conf))

  def bindQueue(exchange: String, queue: String, routingKey: String): Channel => Stream[IO, Unit] =
    send(s"bind queue `$queue`", programs.bindQueue(exchange, queue, routingKey))
}

case class Rabid(queue: Queue[IO, Input])

object Rabid
{
  def native[A]
  (host: String, port: Int, conf: ConnectionConfig)
  (consume: RabidStream[A])
  (implicit ec: ExecutionContext)
  : Stream[IO, A] =
    for {
      connection <- Connection.native(host, port)
      a <- run(consume)(connection, conf)
    } yield a

  def run[A](consume: RabidStream[A])(connection: Connection, conf: ConnectionConfig)
  (implicit ec: ExecutionContext)
  : Stream[IO, A] =
    for {
      (rabid, main) <- Stream.eval(Connection.start(connection, conf))
      a <- consume(rabid).concurrently(main)
    } yield a

  def openChannel(implicit ec: ExecutionContext): RabidIO[Channel] =
    for {
      channel <- RabidIO.liftF(Channel.cons)
      _ <- RabidIO(_.queue.enqueue1(Input.OpenChannel(channel)))
    } yield channel

  def sendToChannel(name: String, prog: ChannelA.Internal): ChannelIO[Unit] =
      ChannelIO(_.exchange.in.enqueue1(ChannelInput.Prog(name, prog)))

  def unitChannel(name: String, prog: ChannelA.Step[Unit]): ChannelIO[Unit] =
    sendToChannel(name, prog.as(PNext.Regular))

  def consumeChannel: ChannelStream[ChannelOutput] =
    ChannelStream(_.exchange.out.dequeue)

  def consumerChannel(name: String, prog: ChannelA.Step[Unit]): ChannelStream[ChannelOutput] =
    unitChannel(name, prog).mapK(StreamUtil.liftIO) >> consumeChannel

  def syncProg[A](prog: ChannelA.Step[A], comm: Queue[IO, A]): ChannelA.Step[Unit] =
    for {
      a <- prog
      _ <- channel.Actions.eval(comm.enqueue1(a))
    } yield ()

  def syncChannel[A](name: String, prog: ChannelA.Step[A])
  (implicit ec: ExecutionContext)
  : ChannelIO[A] =
    for {
      comm <- ChannelIO.liftF(Queue.bounded[IO, A](1))
      _ <- unitChannel(name, syncProg(prog, comm))
      a <- ChannelIO.liftF(comm.dequeue1)
    } yield a

  def publish1[A: Encoder](exchange: String, routingKey: String)(message: A): ChannelIO[Unit] =
    sendToChannel(
      s"publish to `$exchange` as `$routingKey`: $message",
      programs.publish1(exchange, routingKey, message.asJson.spaces2),
    )

  def consumeProg(stop: Signal[IO, Boolean])
  (exchange: ExchangeConf, queue: QueueConf, route: String, ack: Boolean)
  : ChannelA.Step[Unit] =
    for {
      _ <- programs.declareExchange(exchange)
      _ <- programs.declareQueue(queue)
      _ <- programs.bindQueue(exchange.name, queue.name, route)
      _ <- programs.consume(queue.name, stop, ack)
    } yield ()

  def ack[A](message: Message[A]): ChannelIO[Unit] =
    ChannelIO(_.receive.enqueue1(ChannelMessage.Ack(message.deliveryTag, false)))

  def acker[A]: List[Message[A]] => ChannelIO[Unit] =
    _.traverse(ack).void

  def consumeJsonIn[A: Decoder]
  (stop: Signal[IO, Boolean])
  (exchange: ExchangeConf, queue: QueueConf, route: String, ack: Boolean)
  : ChannelStream[Message[A]] =
    for {
      output <- consumerChannel(
        s"consume json from ${exchange.name}:${queue.name}:$route",
        consumeProg(stop)(exchange, queue, route, ack)
      )
      data <- decode[A](output.message.data) match {
        case Right(a) => ChannelStream.pure(Message(a, output.message.deliveryTag))
        case Left(error) =>
          for {
            _ <- ChannelStream.eval(Log.error[IO]("consumeJsonIn", f"failed to decode json message: $error: $output"))
            a <- ChannelStream.liftF[Message[A]](Stream.empty)
          } yield a
      }
    } yield data
}

object io
{
  def publishJson[A: Encoder](exchange: String, routingKey: String)(messages: List[A])
  (implicit ec: ExecutionContext)
  : RabidIO[Unit] =
    for {
      channel <- Rabid.openChannel
      _ <- RabidIO.liftF(messages.traverse(Rabid.publish1(exchange, routingKey)).void(channel))
    } yield ()

  def consumeJson[A: Decoder](exchange: ExchangeConf, queue: QueueConf, route: String, ack: Boolean)
  (implicit ec: ExecutionContext)
  : RabidIO[(List[Message[A]] => IO[Unit], Stream[IO, Message[A]])] =
    for {
      stop <- RabidIO.liftF(Signals.event)
      channel <- Rabid.openChannel
    } yield (a => Rabid.acker[A](a)(channel), Rabid.consumeJsonIn[A](stop)(exchange, queue, route, ack).apply(channel))
}
