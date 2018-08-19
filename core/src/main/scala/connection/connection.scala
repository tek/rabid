package rabid
package connection

import java.nio.channels.AsynchronousChannelGroup

import scala.concurrent.ExecutionContext
import scala.collection.immutable.SortedMap

import fs2.{Stream, Pull}
import fs2.async.mutable.{Signal, Queue}
import scodec.{Attempt, Err}
import cats.~>
import cats.data.{EitherT, StateT}
import cats.effect.IO
import cats.free.Free
import cats.implicits._

import channel.{ChannelConnection, Channel, ChannelProg, programs}

case class Connection(
  pool: Connection.ChannelPool,
  channel0: ChannelConnection,
  channels: SortedMap[Short, ChannelConnection],
  state: ConnectionState,
  connected: Signal[IO, Boolean],
)

object Connection
{
  type ChannelPool = Queue[IO, Stream[IO, Input]]

  def sendToRabbit(message: Message): Action.Step[ActionResult] =
    Action.liftF(Action.Send(message)).as(ActionResult.Continue)

  def sendToChannel(header: FrameHeader, body: FrameBody): Action.Step[ActionResult] =
    Action.liftF(Action.SendToChannel(header, body)).as(ActionResult.Continue)

  def consumerRequest(channel: Channel): Action.Step[ActionResult] =
    Action.liftF(Action.CreateChannel(channel)).as(ActionResult.Continue)

  def channelCreated(number: Short, id: String): Action.Step[ActionResult] =
    Action.liftF(Action.ChannelCreated(number, id)).as(ActionResult.Continue)

  def send: Input => Action.Step[ActionResult] = {
    case Input.Connected => Free.pure(ActionResult.Connected)
    case Input.Rabbit(message) => sendToRabbit(message)
    case Input.SendToChannel(header, body) => sendToChannel(header, body)
    case Input.CreateChannel(request) => consumerRequest(request)
    case Input.ChannelCreated(number, id) => channelCreated(number, id)
  }

  def listen: Action.Step[ActionResult] =
    for {
      comm <- Action.liftF(Action.Listen)
      result <- send(comm)
    } yield result

  def exit: Action.Step[ActionResult] =
    Free.pure(ActionResult.Done)

  def connected: Action.Step[ActionResult] =
    for {
      _ <- Action.liftF(Action.SetConnected(true))
      _ <- Action.liftF(Action.RunInControlChannel(ChannelProg("listen in control channel", programs.controlListen)))
    } yield ActionResult.Running

  def act: ConnectionState => Action.Step[ActionResult] = {
    case ConnectionState.Disconnected =>
      for {
        _ <- Action.liftF(Action.StartControlChannel)
        _ <- Action.liftF(Action.RunInControlChannel(ChannelProg("connect to server", programs.connect)))
      } yield ActionResult.Started
    case ConnectionState.Connecting => listen
    case ConnectionState.Connected => connected
    case ConnectionState.Running => listen
    case ConnectionState.Done => exit
  }

  def transition(state: ConnectionState): ActionResult => Option[ConnectionState] = {
    case ActionResult.Started => Some(ConnectionState.Connecting)
    case ActionResult.Connected => Some(ConnectionState.Connected)
    case ActionResult.Running => Some(ConnectionState.Running)
    case ActionResult.Continue => Some(state)
    case ActionResult.Done => None
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

  def step(interpreter: Action ~> Action.Effect)(data: Connection)
  : Action.Pull[(Connection, Either[Err, ActionResult])] =
    act(data.state).foldMap(interpretAttempt(interpreter)).value.run(data)

  def process(interpreter: Action ~> Action.Effect)(data: Connection): Action.Pull[Option[Connection]] =
    for {
      result <- step(interpreter)(data)
      output <- result match {
        case (data, Right(result)) =>
          Pull.pure(transition(data.state)(result).map(a => data.copy(state = a)))
        case (data, Left(err)) =>
          Log.pull.error("connection", s"error in connection program: $err").as(Some(data))
      }
    } yield output

  def run(
    pool: Connection.ChannelPool,
    interpreter: Action ~> Action.Effect,
    connected: Signal[IO, Boolean],
  )
  (implicit ec: ExecutionContext)
  : Stream[IO, Unit] =
    for {
      channel0 <- Stream.eval(Channel.cons)
      connection0 <- Stream.eval(ChannelConnection.cons(0, channel0, connected))
      _ <- Pull.loop(
        process(interpreter))(
          Connection(pool, connection0, SortedMap.empty, ConnectionState.Disconnected, connected)
      ).stream
    } yield ()

  def native
  (host: String, port: Int)
  (implicit ec: ExecutionContext, ag: AsynchronousChannelGroup)
  : Stream[IO, (Rabid, Stream[IO, Unit], IO[Unit])] =
    for {
      (pool, listener, input, connected, interpreter, close) <- Interpreter.native(host, port)
    } yield (Rabid(input), run(pool, interpreter, connected).merge(listener.drain), close)
}
