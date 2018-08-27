package rabid

import scala.concurrent.ExecutionContext

import fs2.Stream
import cats.data.Kleisli
import cats.effect.IO
import _root_.io.circe.{Encoder, Decoder}

import channel.Channel

object `package`
{
  type RabidOp[F[_], A] = Kleisli[F, Rabid, A]
  type RabidIO[A] = Kleisli[IO, Rabid, A]
  type RabidStream[A] = Kleisli[Stream[IO, ?], Rabid, A]
  type ChannelOp[F[_], A] = Kleisli[F, Channel, A]
  type ChannelIO[A] = Kleisli[IO, Channel, A]
  type ChannelStream[A] = Kleisli[Stream[IO, ?], Channel, A]

  def publishJson[A: Encoder](exchange: String, route: String)(messages: List[A])
  (implicit ec: ExecutionContext)
  : RabidStream[Unit] =
    RabidStream.liftIO(io.publishJson[A](exchange, route)(messages))

  def consumeJson[A: Decoder](exchange: String, queue: String, route: String, ack: Boolean)
  (implicit ec: ExecutionContext)
  : RabidStream[(List[Message[A]] => IO[Unit], Stream[IO, Message[A]])] =
    RabidStream.liftIO(io.consumeJson[A](exchange, queue, route, ack))
}
