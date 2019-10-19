package rabid

import cats.effect.{ContextShift, IO}
import fs2.Stream
import fs2.concurrent.{Signal, SignallingRef}

object Signals
{
  def blockedBy[A](signal: Signal[IO, Boolean])(stream: Stream[IO, A])
  (implicit cs: ContextShift[IO])
  : Stream[IO, A] =
    stream.pauseWhen(signal.map(!_))

  def event
  (implicit cs: ContextShift[IO])
  : IO[Signal[IO, Boolean]] =
    SignallingRef[IO, Boolean](false)

  def eventS
  (implicit cs: ContextShift[IO])
  : Stream[IO, Signal[IO, Boolean]] =
    Stream.eval(event)
}
