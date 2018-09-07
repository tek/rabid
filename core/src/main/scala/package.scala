package rabid

import scala.concurrent.ExecutionContext

import fs2.Stream
import cats.data.Kleisli
import cats.implicits._
import cats.effect.IO
import _root_.io.circe.{Encoder, Decoder}

import channel.{Channel, ExchangeConf, QueueConf, DeliveryTag}

object `package`
{
  type RabidOp[F[_], A] = Kleisli[F, Rabid, A]
  type RabidIO[A] = Kleisli[IO, Rabid, A]
  type RabidStream[A] = Kleisli[Stream[IO, ?], Rabid, A]
  type ChannelOp[F[_], A] = Kleisli[F, Channel, A]
  type ChannelIO[A] = Kleisli[IO, Channel, A]
  type ChannelStream[A] = Kleisli[Stream[IO, ?], Channel, A]

  def openChannel(implicit ec: ExecutionContext): RabidIO[Channel] =
    Rabid.openChannel

  def publish(exchange: ExchangeConf, route: String)(messages: List[String])
  : ChannelStream[Unit] =
    ChannelStream.liftIO(messages.traverse(Rabid.publish1(exchange, route)).void)

  def publishJsonIn[A: Encoder](exchange: ExchangeConf, route: String)(messages: List[A])
  : ChannelStream[Unit] =
    ChannelStream.liftIO(messages.traverse(Rabid.publishJson1(exchange, route)).void)

  def publishJson[A: Encoder](exchange: ExchangeConf, route: String)(messages: List[A])
  (implicit ec: ExecutionContext)
  : RabidStream[Unit] =
    RabidStream.liftIO(io.publishJson[A](exchange, route)(messages))

  def consumeJson[A: Decoder](exchange: ExchangeConf, queue: QueueConf, route: String, ack: Boolean)
  (implicit ec: ExecutionContext)
  : RabidStream[(List[DeliveryTag] => IO[Unit], Stream[IO, Consume[A]])] =
    RabidStream.liftIO(io.consumeJson[A](exchange, queue, route, ack))
}
