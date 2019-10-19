package rabid

import cats.data.Kleisli
import cats.effect.IO
import fs2.Stream

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
