package rabid

import scala.concurrent.ExecutionContext

import fs2.Stream
import cats.data.Kleisli
import cats.implicits._
import cats.effect.IO
import io.circe.{Encoder, Decoder}

import channel.Channel

object `package`
{
  type RabidOp[F[_], A] = Kleisli[F, Rabid, A]
  type RabidIO[A] = Kleisli[IO, Rabid, A]
  type RabidStream[A] = Kleisli[Stream[IO, ?], Rabid, A]
  type ChannelOp[F[_], A] = Kleisli[F, Channel, A]
  type ChannelIO[A] = Kleisli[IO, Channel, A]
  type ChannelStream[A] = Kleisli[Stream[IO, ?], Channel, A]

  def publish[A: Encoder](exchange: String, routingKey: String)(messages: List[A])
  (implicit ec: ExecutionContext)
  : RabidIO[Unit] =
    for {
      channel <- Rabid.openChannel
      _ <- RabidIO.liftF(messages.traverse(Rabid.publish1(exchange, routingKey)).void(channel))
    } yield ()

  def consumeJson[A: Decoder](exchange: String, queue: String, route: String, ack: Boolean)
  (implicit ec: ExecutionContext)
  : RabidIO[(List[Message[A]] => IO[Unit], Stream[IO, Message[A]])] =
    for {
      stop <- RabidIO.liftF(Signals.event)
      channel <- Rabid.openChannel
    } yield (Rabid.acker(channel), Rabid.consumeJsonIn[A](stop)(exchange, queue, route, ack).apply(channel))
}
