package rabid

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import fs2.{Stream, Scheduler}
import cats.~>
import cats.effect.IO
import cats.implicits._

object StreamUtil
{
  def timed[A](max: FiniteDuration)(s: Stream[IO, A])
  (implicit ec: ExecutionContext)
  : Stream[IO, A] =
    for {
      scheduler <- Scheduler[IO](1)
      a <- s.mergeHaltR(scheduler.sleep[IO](max).drain)
    } yield a

  def liftIO: IO ~> Stream[IO, ?] =
    λ[IO ~> Stream[IO, ?]](Stream.eval(_))
}
