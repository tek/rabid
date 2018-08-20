package rabid
package channel

import scodec.{Encoder, Decoder, DecodeResult, Codec, Err}
import scodec.bits.{ByteVector, BitVector}
import scodec.codecs.utf8
import cats.data.EitherT
import cats.effect.IO
import cats.free.Free
import fs2.async.mutable.Signal

import connection.Input

sealed trait Action[A]

object Action
{
  type Attempt[A] = scodec.Attempt[Action[A]]
  type Step[A] = Free[Attempt, A]
  type Pull[A] = fs2.Pull[IO, Input, A]
  type Effect[A] = EitherT[Pull, Err, A]

  case object SendAmqpHeader
  extends Action[Unit]

  case object Receive
  extends Action[ByteVector]

  case class SendMethod(payload: ByteVector)
  extends Action[Unit]

  case class SendContent(classId: Short, payload: ByteVector)
  extends Action[Unit]

  case object ReceiveContent
  extends Action[ByteVector]

  case class NotifyConsumer(signal: Signal[IO, Option[Either[String, String]]], data: Either[String, String])
  extends Action[Unit]

  case class Log(message: String)
  extends Action[Unit]

  case class Output(comm: Input)
  extends Action[Unit]

  case object ChannelCreated
  extends Action[Unit]

  def liftF[A](a: Action[A]): Step[A] =
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
    def pull[A](p: Pull[A]): Effect[A] =
      EitherT.liftF(p)

    def eval[A](fa: IO[A]): Effect[A] =
      pull(fs2.Pull.eval(fa))

    def either[A](a: Either[Err, A]): Effect[A] =
      EitherT.fromEither[Pull](a)

    def attempt[A](a: scodec.Attempt[A]): Effect[A] =
      either(a.toEither)
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
  import Action._

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

  def notifyConsumer(signal: Signal[IO, Option[Either[String, String]]], data: Either[String, String]): Step[Unit] =
    liftF(NotifyConsumer(signal, data))

  def receiveContent: Step[ByteVector] = liftF(ReceiveContent)

  def log[A](message: A): Step[Unit] =
    liftF(Log(message.toString))

  def channelCreated: Step[Unit] = liftF(ChannelCreated)

  def output(comm: Input): Step[Unit] = liftF(Output(comm))
}
