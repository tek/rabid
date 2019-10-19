package rabid

import cats.data.Kleisli
import cats.effect.IO
import channel.Channel

object ChannelIO
{
  def apply[A](fa: Channel => IO[A]): ChannelIO[A] =
    Kleisli(fa)

  def pure[A](a: A): ChannelIO[A] =
    Kleisli.pure(a)

  def liftF[A](fa: IO[A]): ChannelIO[A] =
    Kleisli.liftF(fa)
}
