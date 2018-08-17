package rabid
package connection

import java.nio.channels.AsynchronousChannelGroup

import scala.concurrent.ExecutionContext
import scala.collection.immutable.SortedMap

import fs2.{Stream, Pull}
import fs2.async.mutable.Signal
import scodec.{Encoder, Decoder, Attempt, DecodeResult, Err}
import scodec.bits.{ByteVector, BitVector}
import cats.~>
import cats.data.{EitherT, StateT}
import cats.effect.IO
import cats.free.Free
import cats.implicits._

import channel.{ChannelConnection, Channel, ChannelProg, programs}

sealed trait Action[A]

object Action
{
  type Attempt[A] = scodec.Attempt[Action[A]]
  type Step[A] = Free[Attempt, A]
  type Pull[A] = fs2.Pull[IO, Nothing, A]
  type State[A] = StateT[Pull, ConnectionData, A]
  type Effect[A] = EitherT[State, Err, A]

  case object Listen
  extends Action[Communicate]

  case class SetConnected(state: Boolean)
  extends Action[Unit]

  case class Send(payload: Message)
  extends Action[Unit]

  case object StartControlChannel
  extends Action[Unit]

  case class RunInControlChannel(action: ChannelProg)
  extends Action[Unit]

  case class SendToChannel(header: FrameHeader, body: FrameBody)
  extends Action[Unit]

  case class CreateChannel(channel: Channel)
  extends Action[Unit]

  case class ChannelCreated(number: Short, id: String)
  extends Action[Unit]

  case class Log(message: String)
  extends Action[Unit]

  def liftF[A](a: Action[A]): Step[A] =
    Free.liftF[Action.Attempt, A](scodec.Attempt.Successful(a))

  def fromOption[A](a: Option[A]): Step[A] =
    a
      .map(Free.pure[Action.Attempt, A])
      .getOrElse(Free.liftF[Action.Attempt, A](scodec.Attempt.Failure(Err.General("", Nil))))

  def fromAttempt[A](fa: scodec.Attempt[A]): Step[A] =
    fa match {
      case scodec.Attempt.Successful(a) => Free.pure(a)
      case scodec.Attempt.Failure(e) => Free.liftF[Action.Attempt, A](scodec.Attempt.Failure(e))
    }

  def decode[A: Decoder](bits: BitVector): Step[DecodeResult[A]] =
    Action.fromAttempt(Decoder[A].decode(bits))

  def decodeBytes[A: Decoder](bytes: ByteVector): Step[DecodeResult[A]] =
    decode[A](BitVector(bytes))

  def encode[A: Encoder](a: A): Step[BitVector] =
    Action.fromAttempt(Encoder[A].encode(a))

  def encodeBytes[A: Encoder](a: A): Step[ByteVector] =
    encode(a).map(_.toByteVector)

  def log[A](message: A): Step[Unit] =
    Action.liftF(Action.Log(message.toString))

  object Effect
  {
    def pure[A](a: A): Effect[A] =
      EitherT.liftF(StateT.liftF(Pull.pure(a)))

    def pull[A](p: Pull[A]): Effect[A] =
      EitherT.liftF(StateT.liftF(p))

    def eval[A](fa: IO[A]): Effect[A] =
      pull(fs2.Pull.eval(fa))

    def either[A](a: Either[Err, A]): Effect[A] =
      EitherT.fromEither[State](a)
  }

  object State
  {
    def pure[A](a: A): State[A] =
      StateT.liftF(Pull.pure(a))

    def inspect[A](f: ConnectionData => A): State[A] =
      StateT.inspect(f)

    def modify(f: ConnectionData => ConnectionData): State[Unit] =
      StateT.modify(f)

    def pull[A](p: Pull[A]): State[A] =
      StateT.liftF(p)

    def eval[A](fa: IO[A]): State[A] =
      pull(fs2.Pull.eval(fa))
  }
}

object Connection
{
  def sendToRabbit(message: Message): Action.Step[ActionResult] =
    for {
      _ <- Action.liftF(Action.Send(message))
    } yield ActionResult.Continue

  def sendToChannel(header: FrameHeader, body: FrameBody): Action.Step[ActionResult] =
    for {
      _ <- Action.liftF(Action.SendToChannel(header, body))
    } yield ActionResult.Continue

  def consumerRequest: ConsumerRequest => Action.Step[ActionResult] = {
    case ConsumerRequest.CreateChannel(channel) =>
      for {
        _ <- Action.liftF(Action.CreateChannel(channel))
      } yield ActionResult.Continue
  }

  def channelCreated(number: Short, id: String): Action.Step[ActionResult] =
    for {
      _ <- Action.liftF(Action.ChannelCreated(number, id))
    } yield ActionResult.Continue

  def send: Communicate => Action.Step[ActionResult] = {
    case Communicate.Connected => Free.pure(ActionResult.Connected)
    case Communicate.Rabbit(message) => sendToRabbit(message)
    case Communicate.Channel(header, body) => sendToChannel(header, body)
    case Communicate.Request(request) => consumerRequest(request)
    case Communicate.ChannelCreated(number, id) => channelCreated(number, id)
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

  def process(interpreter: Action ~> Action.Effect)(data: ConnectionData)
  : Pull[IO, Nothing, Option[ConnectionData]] = {
    act(data.state).foldMap(interpretAttempt(interpreter)).value.run(data).map {
      case (data, Right(result)) =>
        transition(data.state)(result).map(a => data.copy(state = a))
      case (_, Left(err)) =>
        println(s"loop exiting: $err")
        None
        // Some(ConnectionState.Disconnected)
    }
  }

  def run(
    pool: ConnectionData.ChannelPool,
    interpreter: Action ~> Action.Effect,
    connected: Signal[IO, Boolean],
  )
  (implicit ec: ExecutionContext)
  : Stream[IO, Unit] =
    for {
      channel0 <- Stream.eval(Channel.cons)
      connection0 <- Stream.eval(ChannelConnection.cons(0, channel0, connected))
      _ <- Pull.loop(
        process(interpreter))(ConnectionData(pool, connection0, SortedMap.empty, ConnectionState.Disconnected, connected)
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
