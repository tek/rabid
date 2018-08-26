package rabid

import cats.data.Kleisli
import cats.effect.IO
import fs2.Stream

import channel.Channel

object RabidStream
{
  def apply[A](fa: Rabid => Stream[IO, A]): RabidStream[A] =
    Kleisli(fa)

  def pure[A](a: A): RabidStream[A] =
    Kleisli.pure(a)

  def liftIO[A](fa: RabidIO[A]): RabidStream[A] =
    fa.mapK(StreamUtil.liftIO)

  def liftF[A](fa: Stream[IO, A]): RabidStream[A] =
    Kleisli.liftF(fa)

  def eval[A](fa: IO[A]): RabidStream[A] =
    liftF(Stream.eval(fa))
}

object RabidIO
{
  def apply[A](fa: Rabid => IO[A]): RabidIO[A] =
    Kleisli(fa)

  def pure[A](a: A): RabidIO[A] =
    Kleisli.pure(a)

  def liftF[A](fa: IO[A]): RabidIO[A] =
    Kleisli.liftF(fa)
}

object ChannelIO
{
  def apply[A](fa: Channel => IO[A]): ChannelIO[A] =
    Kleisli(fa)

  def pure[A](a: A): ChannelIO[A] =
    Kleisli.pure(a)

  def liftF[A](fa: IO[A]): ChannelIO[A] =
    Kleisli.liftF(fa)
}

object ChannelStream
{
  def apply[A](fa: Channel => Stream[IO, A]): ChannelStream[A] =
    Kleisli(fa)

  def pure[A](a: A): ChannelStream[A] =
    Kleisli.pure(a)

  def liftIO[A](fa: ChannelIO[A]): ChannelStream[A] =
    fa.mapK(StreamUtil.liftIO)

  def liftF[A](fa: Stream[IO, A]): ChannelStream[A] =
    Kleisli.liftF(fa)

  def eval[A](fa: IO[A]): ChannelStream[A] =
    liftF(Stream.eval(fa))
}
