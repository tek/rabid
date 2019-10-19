package rabid

import cats.data.Kleisli
import cats.effect.IO

object RabidIO
{
  def apply[A](fa: Rabid => IO[A]): RabidIO[A] =
    Kleisli(fa)

  def pure[A](a: A): RabidIO[A] =
    Kleisli.pure(a)

  def liftF[A](fa: IO[A]): RabidIO[A] =
    Kleisli.liftF(fa)
}
