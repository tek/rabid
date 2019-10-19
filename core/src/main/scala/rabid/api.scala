package rabid

import _root_.io.circe.{Decoder, Encoder, Error}
import _root_.io.circe.parser._
import _root_.io.circe.syntax._
import cats.Traverse
import cats.effect.{ContextShift, IO}
import cats.implicits._
import channel.{
  Channel,
  ChannelA,
  ChannelInput,
  ChannelMessage,
  ChannelOutput,
  Delivery,
  DeliveryTag,
  ExchangeConf,
  QueueConf,
  programs
}
import connection.{Connection, Input}
import fs2.Stream
import fs2.concurrent.{Enqueue, Queue, Signal}

sealed trait Consume[A]

object Consume
{
  case class Message[A](data: A, tag: DeliveryTag)
  extends Consume[A]

  case class JsonError[A](delivery: Delivery, error: Error)
  extends Consume[A]
}

case class ServerConf(host: String, port: Short)

case class ConnectionConf(user: String, password: String, vhost: String)

case class QosConf(prefetchSize: Int, prefetchCount: Short)

case class RabidConf(server: ServerConf, connection: ConnectionConf, qos: QosConf)

object Api
{
  def send(name: String, thunk: Channel.Prog)(channel: Channel): Stream[IO, Unit] =
    Stream.eval(channel.exchange.in.enqueue1(ChannelInput.Prog(name, thunk)))

  def declareExchange(conf: ExchangeConf): Channel => Stream[IO, Unit] =
    send(s"declare exchange `${conf.name}`", programs.declareExchange(conf))

  def bindQueue(exchange: String, queue: String, routingKey: String): Channel => Stream[IO, Unit] =
    send(s"bind queue `$queue`", programs.bindQueue(exchange, queue, routingKey))
}

case class Rabid(queue: Enqueue[IO, Input])

object Rabid
{
  def native[A]
  (conf: RabidConf)
  (consume: RabidStream[A])
  (implicit cs: ContextShift[IO])
  : Stream[IO, A] =
    for {
      connection <- Stream.resource(Connection.native(conf))
      a <- run(consume)(connection)
    } yield a

  def run[A](consume: RabidStream[A])(connection: Connection)
  (implicit cs: ContextShift[IO])
  : Stream[IO, A] =
    for {
      (rabid, main) <- Stream.eval(Connection.start(connection))
      a <- consume(rabid).concurrently(main)
    } yield a

  def openChannel(implicit cs: ContextShift[IO]): RabidIO[Channel] =
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
  (implicit cs: ContextShift[IO])
  : ChannelIO[A] =
    for {
      comm <- ChannelIO.liftF(Queue.bounded[IO, A](1))
      _ <- unitChannel(name, syncProg(prog, comm))
      a <- ChannelIO.liftF(comm.dequeue1)
    } yield a

  def publish1(exchange: ExchangeConf, routingKey: String)(message: String): ChannelIO[Unit] =
    sendToChannel(
      s"publish to `$exchange` as `$routingKey`: $message",
      for {
      _ <- programs.declareExchange(exchange)
      a <- programs.publish1(exchange.name, routingKey, message)
      } yield a
    )

  def publishJson1[A: Encoder](exchange: ExchangeConf, routingKey: String)(message: A): ChannelIO[Unit] =
    publish1(exchange, routingKey)(message.asJson.spaces2)

  def publishJson[F[_]: Traverse, A: Encoder]
  (exchange: ExchangeConf, routingKey: String)
  (messages: F[A])
  : ChannelIO[Unit] =
    sendToChannel(
      s"publish ${messages.size} messages to `$exchange` as `$routingKey`",
      for {
      _ <- programs.declareExchange(exchange)
      _ <- messages.traverse(message => programs.publish1(exchange.name, routingKey, message.asJson.spaces2))
      } yield PNext.Regular
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

  def ack[A](tag: DeliveryTag): ChannelIO[Unit] =
    ChannelIO(_.receive.enqueue1(ChannelMessage.Ack(tag.data, false)))

  def acker[A]: List[DeliveryTag] => ChannelIO[Unit] =
    _.traverse(ack).void

  def consumeJsonIn[A: Decoder]
  (stop: Signal[IO, Boolean])
  (exchange: ExchangeConf, queue: QueueConf, route: String, ack: Boolean)
  : ChannelStream[Consume[A]] =
    for {
      output <- consumerChannel(
        s"consume json from ${exchange.name}:${queue.name}:$route",
        consumeProg(stop)(exchange, queue, route, ack)
      )
      data <- decode[A](output.message.data) match {
        case Right(a) => ChannelStream.pure(Consume.Message(a, output.message.tag))
        case Left(error) =>
          for {
            _ <- ChannelStream.eval(Log.error[IO]("consume.json", f"failed to decode json message: $error: $output"))
          } yield Consume.JsonError[A](output.message, error)
      }
    } yield data
}

object io
{
  def publishJson[A: Encoder](exchange: ExchangeConf, routingKey: String)(messages: List[A])
  (implicit cs: ContextShift[IO])
  : RabidIO[Unit] =
    for {
      channel <- Rabid.openChannel
      _ <- RabidIO.liftF(messages.traverse(Rabid.publishJson1(exchange, routingKey)).void(channel))
    } yield ()

  def consumeJson[A: Decoder](exchange: ExchangeConf, queue: QueueConf, route: String, ack: Boolean)
  (implicit cs: ContextShift[IO])
  : RabidIO[(List[DeliveryTag] => IO[Unit], Stream[IO, Consume[A]])] =
    for {
      stop <- RabidIO.liftF(Signals.event)
      channel <- Rabid.openChannel
    } yield (a => Rabid.acker[A](a)(channel), Rabid.consumeJsonIn[A](stop)(exchange, queue, route, ack).apply(channel))
}
