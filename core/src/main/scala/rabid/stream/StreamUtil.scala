package rabid

import scala.concurrent.duration.FiniteDuration

import cats.~>
import cats.effect.{ContextShift, IO, Timer}
import fs2.Stream

object StreamUtil
{
  def timed[A](max: FiniteDuration)(s: Stream[IO, A])
  (implicit timer: Timer[IO], cs: ContextShift[IO])
  : Stream[IO, A] =
    for {
      a <- s.mergeHaltR(Stream.sleep[IO](max).drain)
    } yield a

  def liftIO: IO ~> Stream[IO, ?] =
    λ[IO ~> Stream[IO, ?]](Stream.eval(_))
}
