package rabid
package channel

import scala.concurrent.ExecutionContext

import fs2.{Stream, Pull}
import fs2.async.mutable.{Queue, Signal}
import scodec.{Attempt, Err}
import scodec.bits.ByteVector
import cats.~>
import cats.syntax.flatMap._
import cats.data.EitherT
import cats.effect.IO

import connection.{Communicate, Exchange, ConsumerResponse}

case class ChannelOutput(number: Short)

case class ChannelProg(name: String, thunk: Channel.Thunk)

case class ChannelConnection(
  number: Short,
  progs: Queue[IO, ChannelProg],
  consumerProgs: Queue[IO, ChannelProg],
  receive: Queue[IO, ByteVector],
  connected: Signal[IO, Boolean],
  created: Signal[IO, Boolean],
)

object ChannelConnection
{
  def cons(number: Short, channel: Channel, connected: Signal[IO, Boolean])
  (implicit ec: ExecutionContext)
  : IO[ChannelConnection] = {
    for {
      in <- Queue.unbounded[IO, ChannelProg]
      input <- Queue.unbounded[IO, ByteVector]
      created <- Signal[IO, Boolean](false)
    } yield ChannelConnection(number, in, channel.exchange.in, input, connected, created)
  }
}

case class Channel(exchange: Exchange[ChannelProg, ConsumerResponse])

object Channel
{
  def cons(implicit ec: ExecutionContext): IO[Channel] =
    for {
      in <- Queue.unbounded[IO, ChannelProg]
      out <- Queue.unbounded[IO, ConsumerResponse]
    } yield Channel(Exchange(in, out))

  type Thunk = Action.Step[ActionResult]

  def interpretAttempt(inner: Action ~> Action.Effect): Action.Attempt ~> Action.Effect =
    new (Action.Attempt ~> Action.Effect) {
      def apply[A](a: Action.Attempt[A]): Action.Effect[A] = {
        a match {
          case Attempt.Successful(a) => inner(a)
          case Attempt.Failure(err) => EitherT[Action.Pull, Err, A](Pull.pure(Left(err)))
        }
      }
    }

  def transition: ActionResult => Option[Unit] = {
    case ActionResult.Continue => Some(())
    case ActionResult.Done => None
  }

  def runJob(interpreter: Action ~> Action.Effect)(prog: ChannelProg): Action.Pull[Option[Unit]] = {
    val thunk = Action.liftF(Action.Log(s"running prog `${prog.name}`")) >> prog.thunk
    for {
      output <- thunk.foldMap(interpretAttempt(interpreter)).value
      next <- output match {
        case Right(a) =>
          Pull.pure(transition(a))
        case Left(err) =>
          Log.pull.error("channel", s"error in channel program: $err").as(None)
      }
    } yield next
  }
  def process(interpreter: Action ~> Action.Effect)(input: Stream[IO, ChannelProg])
  : Pull[IO, Communicate, Unit] =
    for {
      element <- input.pull.uncons1
      a <- element match {
        case Some((prog, remainder)) => runJob(interpreter)(prog) >> process(interpreter)(remainder)
        case None => Pull.done
      }
    } yield a

  def blockedBy[A](signal: Signal[IO, Boolean])(stream: Stream[IO, A])
  (implicit ec: ExecutionContext)
  : Stream[IO, A] =
    stream.pauseWhen(signal.map(!_))

  def channelInput(channel: ChannelConnection)
  (implicit ec: ExecutionContext)
  : Stream[IO, ChannelProg] =
    channel.progs.dequeue.merge(channel.consumerProgs.dequeue.pauseWhen(channel.created.map(!_)))

  def run(channel: ChannelConnection)
  (implicit ec: ExecutionContext)
  : Stream[IO, Communicate] =
    process(Interpreter.interpreter(channel))(channelInput(channel)).stream
}
