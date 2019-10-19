package rabid

import cats.data.Kleisli
import cats.effect.{ContextShift, IO}
import cats.implicits._
import channel.{Channel, DeliveryTag, ExchangeConf, QueueConf}
import fs2.Stream
import io.circe.{Decoder, Encoder}

object `package`
{
  type RabidOp[F[_], A] = Kleisli[F, Rabid, A]
  type RabidIO[A] = Kleisli[IO, Rabid, A]
  type RabidStream[A] = Kleisli[Stream[IO, ?], Rabid, A]
  type ChannelOp[F[_], A] = Kleisli[F, Channel, A]
  type ChannelIO[A] = Kleisli[IO, Channel, A]
  type ChannelStream[A] = Kleisli[Stream[IO, ?], Channel, A]

  def openChannel(implicit cs: ContextShift[IO]): RabidIO[Channel] =
    Rabid.openChannel

  def publish(exchange: ExchangeConf, route: String)(messages: List[String])
  : ChannelStream[Unit] =
    ChannelStream.liftIO(messages.traverse(Rabid.publish1(exchange, route)).void)

  def publishJsonIn[A: Encoder](exchange: ExchangeConf, route: String)(messages: List[A])
  : ChannelStream[Unit] =
    ChannelStream.liftIO(Rabid.publishJson(exchange, route)(messages))

  def publishJson[A: Encoder](exchange: ExchangeConf, route: String)(messages: List[A])
  (implicit cs: ContextShift[IO])
  : RabidStream[Unit] =
    RabidStream.liftIO(IOApi.publishJson[A](exchange, route)(messages))

  def consumeJson[A: Decoder](exchange: ExchangeConf, queue: QueueConf, route: String, ack: Boolean)
  (implicit cs: ContextShift[IO])
  : RabidStream[(List[DeliveryTag] => IO[Unit], Stream[IO, Consume[A]])] =
    RabidStream.liftIO(IOApi.consumeJson[A](exchange, queue, route, ack))
}
