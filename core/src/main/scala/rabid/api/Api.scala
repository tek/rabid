package rabid

import cats.effect.{ContextShift, IO}
import cats.implicits._
import channel.{Channel, ChannelInput, DeliveryTag, ExchangeConf, QueueConf, programs}
import fs2.Stream
import io.circe.{Decoder, Encoder}

object Api
{
  def send(name: String, thunk: Channel.Prog)(channel: Channel): Stream[IO, Unit] =
    Stream.eval(channel.exchange.in.enqueue1(ChannelInput.Prog(name, thunk)))

  def declareExchange(conf: ExchangeConf): Channel => Stream[IO, Unit] =
    send(s"declare exchange `${conf.name}`", programs.declareExchange(conf))

  def bindQueue(exchange: String, queue: String, routingKey: String): Channel => Stream[IO, Unit] =
    send(s"bind queue `$queue`", programs.bindQueue(exchange, queue, routingKey))
}

object IOApi
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
