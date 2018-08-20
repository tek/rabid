package rabid
package connection

import java.nio.channels.AsynchronousChannelGroup

import scala.concurrent.ExecutionContext
import scala.collection.immutable.SortedMap

import fs2.{Stream, Pull}
import fs2.async.mutable.Queue
import scodec.{Attempt, Err}
import cats.~>
import cats.data.{EitherT, StateT, State}
import cats.free.Free
import cats.effect.IO
import cats.implicits._

import channel.{ChannelConnection, Channel}

case class Connection(
  pool: Connection.ChannelPool,
  channel0: ChannelConnection,
  channels: SortedMap[Short, ChannelConnection],
  state: ConnectionState,
  buffer: Vector[Input],
)

object Connection
{
  type ChannelPool = Queue[IO, Stream[IO, Input]]

  def cons(
    pool: Connection.ChannelPool,
    connection0: ChannelConnection,
  ): Connection =
    Connection(pool, connection0, SortedMap.empty, ConnectionState.Disconnected, Vector.empty)

  def buffer(a: Input): State[Connection, Unit] =
    State.modify(s => s.copy(buffer = s.buffer :+ a))

  def bufferOnly(a: Input): State[Connection, Action.Step[Continuation]] =
    buffer(a).as(Free.pure(Continuation.Regular))

  def transition(state: ConnectionState): State[Connection, Unit] =
    State.modify(s => s.copy(state = state))

  def operation: Input => Action.Step[Continuation] = {
    case Input.Connected =>
      Free.pure(Continuation.Regular)
    case Input.Rabbit(message) =>
      programs.sendToRabbit(message)
    case Input.SendToChannel(header, body) =>
      programs.sendToChannel(header, body)
    case Input.CreateChannel(request) =>
      programs.createChannel(request)
    case Input.ChannelCreated(number, id) =>
      programs.channelCreated(number, id)
  }

  def disconnected(input: Input): State[Connection, Action.Step[Continuation]] =
      for {
        _ <- buffer(input)
        _ <- transition(ConnectionState.Connecting)
      } yield programs.connect

  def connecting: Input => State[Connection, Action.Step[Continuation]] = {
    case Input.Connected =>
      transition(ConnectionState.Connected).as(programs.connected)
    case input @ Input.Rabbit(_) =>
      State.pure(operation(input))
    case input @ Input.SendToChannel(header, _) if header.channel == 0 =>
      State.pure(operation(input))
    case a =>
      println(s"received $a in connecting state")
      bufferOnly(a)
  }

  def execute: ConnectionState => Input => State[Connection, Action.Step[Continuation]] = {
    case ConnectionState.Disconnected =>
      disconnected
    case ConnectionState.Connecting =>
      connecting
    case ConnectionState.Connected =>
      operation.andThen(State.pure)
  }

  def debuffer: Action.State[Vector[Input]] = {
    for {
      buffered <- Action.State.inspect(_.buffer)
      _ <- {
        if (buffered.isEmpty) Action.State.pure(())
        else
          for {
            _ <- Action.State.modify(_.copy(buffer = Vector.empty))
            _ <- Action.State.pull(Log.pull.info("connection", s"rebuffering inputs $buffered"))
          } yield ()
      }
    } yield buffered
  }

  def interpretAttempt(inner: Action ~> Action.Effect): Action.Attempt ~> Action.Effect =
    new (Action.Attempt ~> Action.Effect) {
      def apply[A](a: Action.Attempt[A]): Action.Effect[A] = {
        a match {
          case Attempt.Successful(a) => inner(a)
          case Attempt.Failure(err) => EitherT[Action.State, Err, A](StateT.liftF(Pull.pure(Left(err))))
        }
      }
    }

  def continuation(tail: Stream[IO, Input]): Either[Err, Continuation] => Action.State[Stream[IO, Input]] = {
    case Right(Continuation.Regular) =>
      Action.State.pure(tail)
    case Right(Continuation.Debuffer) =>
      for {
        debuffered <- debuffer
      } yield Stream.emits(debuffered) ++ tail
    case Right(Continuation.Exit) =>
      Action.State.pure(Stream.empty)
    case Left(err) =>
      Action.State.pull(Log.pull.error("connection", s"error in connection program: $err")).as(tail)
  }

  def interpret
  (interpreter: Action ~> Action.Effect)
  (program: Action.Step[Continuation])
  (tail: Stream[IO, Input])
  : Action.State[Stream[IO, Input]] =
    for {
      output <- program.foldMap(interpretAttempt(interpreter)).value
      cont <- continuation(tail)(output)
    } yield cont

  def process(interpreter: Action ~> Action.Effect)(data: Connection)
  : Option[(Input, Stream[IO, Input])] => Action.Pull[Unit] = {
    case Some((a, tail)) =>
      val (data1, program) = execute(data.state)(a).run(data).value
      for {
        _ <- Log.pull.info("connection", s"received input $a")
        (data2, continuation) <- interpret(interpreter)(program)(tail).run(data1)
        _ <- loop(interpreter)(continuation, data2)
      } yield ()
    case None => Pull.done
  }

  def loop(interpreter: Action ~> Action.Effect)(inputs: Stream[IO, Input], data: Connection)
  : Action.Pull[Unit] =
    for {
      input <- inputs.pull.uncons1
      _ <- process(interpreter)(data)(input)
    } yield ()

  def run(
    pool: Connection.ChannelPool,
    interpreter: Action ~> Action.Effect,
    listen: Stream[IO, Input],
  )
  (implicit ec: ExecutionContext)
  : Stream[IO, Unit] =
    for {
      channel0 <- Stream.eval(Channel.cons)
      connection0 <- Stream.eval(ChannelConnection.cons(0, channel0))
      a <- listen.through(a => loop(interpreter)(a, Connection.cons(pool, connection0)).stream)
    } yield a

  def native
  (host: String, port: Int)
  (implicit ec: ExecutionContext, ag: AsynchronousChannelGroup)
  : Stream[IO, (Rabid, Stream[IO, Unit], IO[Unit])] =
    for {
      (pool, listen, input, interpreter, close) <- Interpreter.native(host, port)
    } yield (Rabid(input), run(pool, interpreter, listen), close)
}
