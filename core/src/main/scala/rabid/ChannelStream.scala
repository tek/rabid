package rabid

import cats.data.Kleisli
import cats.effect.IO
import channel.Channel
import fs2.Stream

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
