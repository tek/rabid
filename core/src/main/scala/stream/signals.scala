package rabid

import scala.concurrent.ExecutionContext

import fs2.Stream
import fs2.async.mutable.Signal
import cats.effect.IO

object Signals
{
  def blockedBy[A](signal: Signal[IO, Boolean])(stream: Stream[IO, A])
  (implicit ec: ExecutionContext)
  : Stream[IO, A] =
    stream.pauseWhen(signal.map(!_))

  def event
  (implicit ec: ExecutionContext)
  : IO[Signal[IO, Boolean]] =
    Signal[IO, Boolean](false)

  def eventS
  (implicit ec: ExecutionContext)
  : Stream[IO, Signal[IO, Boolean]] =
    Stream.eval(event)
}
