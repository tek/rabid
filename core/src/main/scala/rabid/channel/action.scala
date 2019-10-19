package rabid
package channel

import cats.data.{EitherT, Kleisli, StateT}
import cats.effect.IO
import cats.free.Free
import connection.Input
import fs2.Pull
import fs2.concurrent.SignallingRef
import scodec.{Codec, DecodeResult, Decoder, Encoder, Err}
import scodec.bits.{BitVector, ByteVector}
import scodec.codecs.utf8

sealed trait ChannelA[A]

object ChannelA
{
  type Attempt[A] = scodec.Attempt[ChannelA[A]]
  type Step[A] = Free[Attempt, A]
  type Internal = Step[PNext]
  type Pull[A] = fs2.Pull[IO, Input, A]
  type State[A] = StateT[Pull, ChannelData, A]
  type Effect[A] = EitherT[State, Err, A]
  type AKleisli[A] = Kleisli[Effect, ChannelConnection, A]

  case object SendAmqpHeader
  extends ChannelA[Unit]

  case object Receive
  extends ChannelA[ByteVector]

  case class SendMethod(payload: ByteVector)
  extends ChannelA[Unit]

  case class SendContent(classId: Short, payload: ByteVector)
  extends ChannelA[Unit]

  case object ReceiveContent
  extends ChannelA[ByteVector]

  case class NotifyConsumer(signal: SignallingRef[IO, Option[Either[String, String]]], data: Either[String, String])
  extends ChannelA[Unit]

  case class Log(message: String)
  extends ChannelA[Unit]

  case class ConnectionOutput(comm: Input)
  extends ChannelA[Unit]

  case object ChannelOpened
  extends ChannelA[Unit]

  case class Output(data: ChannelOutput)
  extends ChannelA[Unit]

  case class Eval[A](fa: IO[A])
  extends ChannelA[A]

  def liftF[A](a: ChannelA[A]): Step[A] =
    Free.liftF[Attempt, A](scodec.Attempt.Successful(a))

  def pure[A](a: A): Step[A] =
    Free.pure(a)

  def fromOption[A](a: Option[A]): Step[A] =
    a
      .map(pure)
      .getOrElse(Free.liftF[Attempt, A](scodec.Attempt.Failure(Err.General("", Nil))))

  def fromAttempt[A](fa: scodec.Attempt[A]): Step[A] =
    fa match {
      case scodec.Attempt.Successful(a) => Free.pure(a)
      case scodec.Attempt.Failure(e) => Free.liftF[Attempt, A](scodec.Attempt.Failure(e))
    }

  def decode[A: Decoder](bits: BitVector): Step[DecodeResult[A]] =
    fromAttempt(Decoder[A].decode(bits))

  def decodeBytes[A: Decoder](bytes: ByteVector): Step[DecodeResult[A]] =
    decode[A](BitVector(bytes))

  def encode[A: Encoder](a: A): Step[BitVector] =
    fromAttempt(Encoder[A].encode(a))

  def encodeBytes[A: Encoder](a: A): Step[ByteVector] =
    encode(a).map(_.toByteVector)

  object Effect
  {
    def pure[A](a: A): Effect[A] =
      EitherT.liftF(State.pure(a))

    def unit: Effect[Unit] =
      pure(())

    def pull[A](p: Pull[A]): Effect[A] =
      EitherT.liftF(State.pull(p))

    def eval[A](fa: IO[A]): Effect[A] =
      pull(fs2.Pull.eval(fa))

    def either[A](a: Either[Err, A]): Effect[A] =
      EitherT.fromEither[State](a)

    def attempt[A](a: scodec.Attempt[A]): Effect[A] =
      either(a.toEither)
  }

  object State
  {
    def pure[A](a: A): State[A] =
      StateT.liftF(Pull.pure(a))

    def inspect[A](f: ChannelData => A): State[A] =
      StateT.inspect(f)

    def modify(f: ChannelData => ChannelData): State[Unit] =
      StateT.modify(f)

    def pull[A](p: Pull[A]): State[A] =
      StateT.liftF(p)

    def eval[A](fa: IO[A]): State[A] =
      pull(fs2.Pull.eval(fa))
  }

  object AKleisli
  {
    def apply[A](fa: ChannelConnection => Effect[A]): AKleisli[A] =
      Kleisli(fa)

    def pure[A](a: A): AKleisli[A] =
      Kleisli.pure(a)

    def liftF[A](fa: Effect[A]): AKleisli[A] =
      Kleisli.liftF(fa)

    def pull[A](fa: Pull[A]): AKleisli[A] =
      liftF(Effect.pull(fa))

    def eval[A](fa: IO[A]): AKleisli[A] =
      liftF(Effect.eval(fa))

    def attempt[A](a: scodec.Attempt[A]): AKleisli[A] =
      liftF(Effect.attempt(a))

    def applyEval[A](f: ChannelConnection => IO[A]): AKleisli[A] =
      apply(c => Effect.eval(f(c)))

    def inspect[A](f: ChannelConnection => A): AKleisli[A] =
      Kleisli.ask[Effect, ChannelConnection].map(f)
  }
}

sealed trait ActionResult

object ActionResult
{
  case object Continue
  extends ActionResult

  case object Done
  extends ActionResult
}

object Actions
{
  import ChannelA._

  def sendAmqpHeader: Step[Unit] = liftF(SendAmqpHeader)

  def receiveFramePayload[A: Decoder]: Step[A] =
    for {
      bytes <- liftF(Receive)
      payload <- decodeBytes[A](bytes)
    } yield payload.value

  def receiveMethod[A: Codec: ClassId: MethodId]: Step[A] =
    for {
      method <- receiveFramePayload[ClassMethod[A]].map(_.method)
      _ <- log(s"received method $method")
    } yield method

  def sendMethod[A: Codec: ClassId: MethodId](method: A): Step[Unit] =
    for {
      _ <- log(s"sending $method")
      payload <- encodeBytes(ClassMethod(method))
      _ <- liftF(SendMethod(payload))
    } yield ()

  def sendContent(classId: Short, data: String): Step[Unit] =
    for {
      _ <- log("sending content")
      payload <- fromAttempt(utf8.encode(data))
      _ <- liftF(SendContent(classId, payload.toByteVector))
    } yield ()

  def notifyConsumer
  (signal: SignallingRef[IO, Option[Either[String, String]]], data: Either[String, String])
  : Step[Unit] =
    liftF(NotifyConsumer(signal, data))

  def receiveContent: Step[ByteVector] =
    for {
      content <- liftF(ReceiveContent)
      _ <- log(s"received content $content")
    } yield content

  def receiveStringContent: Step[String] =
    for {
      bytes <- liftF(ReceiveContent)
      data <- fromAttempt(utf8.decode(BitVector(bytes)))
      _ <- log(s"received string content ${data.value}")
    } yield data.value

  def log[A](message: A): Step[Unit] =
    liftF(Log(message.toString))

  def channelOpened: Step[Unit] = liftF(ChannelOpened)

  def connectionOutput(comm: Input): Step[Unit] = liftF(ConnectionOutput(comm))

  def output(data: ChannelOutput): Step[Unit] = liftF(Output(data))

  def eval[A](fa: IO[A]): Step[A] = liftF(Eval(fa))
}
