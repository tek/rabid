package rabid
package channel

import scala.concurrent.ExecutionContext

import fs2.{Stream, Pull}
import fs2.async.mutable.Queue
import scodec.{Attempt, Err}
import scodec.bits.ByteVector
import cats.~>
import cats.implicits._
import cats.data.{EitherT, State, StateT}
import cats.free.Free
import cats.effect.IO

import connection.{Exchange, ConsumerResponse, ConnectionState, Continuation, Input}

case class ChannelOutput(number: Short)

sealed trait ChannelInput

object ChannelInput
{
  case class Prog(name: String, thunk: Channel.Thunk)
  extends ChannelInput

  case object Opened
  extends ChannelInput
}

case class ChannelConnection(
  number: Short,
  progs: Queue[IO, ChannelInput],
  consumerProgs: Queue[IO, ChannelInput],
  receive: Queue[IO, ByteVector],
)

object ChannelConnection
{
  def cons(number: Short, channel: Channel)
  (implicit ec: ExecutionContext)
  : IO[ChannelConnection] = {
    for {
      in <- Queue.unbounded[IO, ChannelInput]
      input <- Queue.unbounded[IO, ByteVector]
    } yield ChannelConnection(number, in, channel.exchange.in, input)
  }
}

case class Channel(exchange: Exchange[ChannelInput, ConsumerResponse])

case class ChannelData(number: Short, state: ConnectionState, buffer: Vector[ChannelInput])

object ChannelData
{
  def cons(number: Short, state: ConnectionState): ChannelData =
    ChannelData(number, state, Vector.empty)
}

object Channel
{
  def cons(implicit ec: ExecutionContext): IO[Channel] =
    for {
      in <- Queue.unbounded[IO, ChannelInput]
      out <- Queue.unbounded[IO, ConsumerResponse]
    } yield Channel(Exchange(in, out))

  type Thunk = Action.Step[Continuation]

  // TODO contramap
  def interpretAttempt(inner: Action ~> Action.Effect): Action.Attempt ~> Action.Effect =
    new (Action.Attempt ~> Action.Effect) {
      def apply[A](a: Action.Attempt[A]): Action.Effect[A] = {
        a match {
          case Attempt.Successful(a) => inner(a)
          case Attempt.Failure(err) => EitherT[Action.Pull, Err, A](Pull.pure(Left(err)))
        }
      }
    }

  def buffer(a: ChannelInput): State[ChannelData, Unit] =
    State.modify(s => s.copy(buffer = s.buffer :+ a))

  def bufferOnly(a: ChannelInput): State[ChannelData, Action.Step[Continuation]] =
    buffer(a).as(Free.pure(Continuation.Regular))

  def transition(state: ConnectionState): State[ChannelData, Unit] =
    State.modify(s => s.copy(state = state))

  def disconnected(input: ChannelInput): State[ChannelData, Action.Step[Continuation]] =
    for {
      number <- State.inspect((_: ChannelData).number)
      _ <- buffer(input)
      _ <- transition(ConnectionState.Connecting)
    } yield programs.createChannel(number)

  def connecting: ChannelInput => State[ChannelData, Action.Step[Continuation]] = {
    case ChannelInput.Opened =>
      transition(ConnectionState.Connected).as(Free.pure(Continuation.Debuffer))
    case input => bufferOnly(input)
  }

  def operation: ChannelInput => Action.Step[Continuation] = {
    case ChannelInput.Prog(name, thunk) =>
      Actions.log(s"running channel prog `$name`") >> thunk
    case _ => Action.pure(Continuation.Debuffer)
  }

  def execute: ConnectionState => ChannelInput => State[ChannelData, Action.Step[Continuation]] = {
    case ConnectionState.Disconnected =>
      disconnected
    case ConnectionState.Connecting =>
      connecting
    case ConnectionState.Connected =>
      operation.andThen(State.pure)
  }

  def debuffer: Action.State[Vector[ChannelInput]] = {
    for {
      buffered <- Action.State.inspect(_.buffer)
      _ <- {
        if (buffered.isEmpty) Action.State.pure(())
        else
          for {
            _ <- Action.State.modify(_.copy(buffer = Vector.empty))
            _ <- Action.State.pull(Log.pull.info("channel", s"rebuffering inputs $buffered"))
          } yield ()
      }
    } yield buffered
  }

  def continuation(tail: Stream[IO, ChannelInput]): Either[Err, Continuation] => Action.State[Stream[IO, ChannelInput]] = {
    case Right(Continuation.Regular) =>
      Action.State.pure(tail)
    case Right(Continuation.Debuffer) =>
      for {
        debuffered <- debuffer
      } yield Stream.emits(debuffered) ++ tail
    case Right(Continuation.Exit) =>
      Action.State.pure(Stream.empty)
    case Left(err) =>
      Action.State.pull(Log.pull.error("channel", s"error in channel program: $err")).as(tail)
  }

  def interpret
  (interpreter: Action ~> Action.Effect)
  (program: Action.Step[Continuation])
  (tail: Stream[IO, ChannelInput])
  : Action.State[Stream[IO, ChannelInput]] =
    for {
      output <- StateT.liftF(program.foldMap(interpretAttempt(interpreter)).value)
      cont <- continuation(tail)(output)
    } yield cont

  def process(interpreter: Action ~> Action.Effect)(data: ChannelData)
  : Option[(ChannelInput, Stream[IO, ChannelInput])] => Action.Pull[Unit] = {
    case Some((a, tail)) =>
      val (data1, program) = execute(data.state)(a).run(data).value
      for {
        _ <- Log.pull.info("channel", s"received input $a")
        (data2, continuation) <- interpret(interpreter)(program)(tail).run(data1)
        _ <- loop(interpreter)(continuation, data2)
      } yield ()
    case None => Pull.done
  }

  def loop(interpreter: Action ~> Action.Effect)(inputs: Stream[IO, ChannelInput], data: ChannelData)
  : Pull[IO, Input, Unit] =
    for {
      input <- inputs.pull.uncons1
      _ <- process(interpreter)(data)(input)
    } yield ()

  def channelInput(channel: ChannelConnection)
  (implicit ec: ExecutionContext)
  : Stream[IO, ChannelInput] =
    channel.progs.dequeue.merge(channel.consumerProgs.dequeue)

  def runChannel(channel: ChannelConnection, state: ConnectionState)
  (implicit ec: ExecutionContext)
  : Stream[IO, Input] =
    channelInput(channel)
      .through(a => loop(Interpreter.interpreter(channel))(a, ChannelData.cons(channel.number, state)).stream)

  def runControl(channel: ChannelConnection)
  (implicit ec: ExecutionContext)
  : Stream[IO, Input] =
    runChannel(channel, ConnectionState.Connected)

  def run(channel: ChannelConnection)
  (implicit ec: ExecutionContext)
  : Stream[IO, Input] =
    runChannel(channel, ConnectionState.Disconnected)
}
