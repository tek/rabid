package rabid
package connection

import fs2.Pull
import scodec.{Encoder, Decoder, DecodeResult, Err}
import scodec.bits.{ByteVector, BitVector}
import cats.data.{EitherT, StateT}
import cats.effect.IO
import cats.free.Free

import channel.{Channel, ChannelInput}

sealed trait ConnectionA[A]

object ConnectionA
{
  type Attempt[A] = scodec.Attempt[ConnectionA[A]]
  type Step[A] = Free[Attempt, A]
  type Pull[A] = fs2.Pull[IO, Nothing, A]
  type State[A] = StateT[Pull, ConnectionResources, A]
  type Effect[A] = EitherT[State, Err, A]

  case class Send(payload: Message)
  extends ConnectionA[Unit]

  case object StartControlChannel
  extends ConnectionA[Unit]

  case class RunInControlChannel(action: ChannelInput.Internal)
  extends ConnectionA[Unit]

  case class ChannelReceive(header: FrameHeader, body: FrameBody)
  extends ConnectionA[Unit]

  case class NotifyChannel(number: Short, input: ChannelInput)
  extends ConnectionA[Unit]

  case class OpenChannel(channel: Channel, qos: QosConf)
  extends ConnectionA[Unit]

  case class ChannelOpened(number: Short, id: String)
  extends ConnectionA[Unit]

  case class Log(message: String)
  extends ConnectionA[Unit]

  def liftF[A](a: ConnectionA[A]): Step[A] =
    Free.liftF[ConnectionA.Attempt, A](scodec.Attempt.Successful(a))

  def fromOption[A](a: Option[A]): Step[A] =
    a
      .map(Free.pure[ConnectionA.Attempt, A])
      .getOrElse(Free.liftF[ConnectionA.Attempt, A](scodec.Attempt.Failure(Err.General("", Nil))))

  def fromAttempt[A](fa: scodec.Attempt[A]): Step[A] =
    fa match {
      case scodec.Attempt.Successful(a) => Free.pure(a)
      case scodec.Attempt.Failure(e) => Free.liftF[ConnectionA.Attempt, A](scodec.Attempt.Failure(e))
    }

  def decode[A: Decoder](bits: BitVector): Step[DecodeResult[A]] =
    ConnectionA.fromAttempt(Decoder[A].decode(bits))

  def decodeBytes[A: Decoder](bytes: ByteVector): Step[DecodeResult[A]] =
    decode[A](BitVector(bytes))

  def encode[A: Encoder](a: A): Step[BitVector] =
    ConnectionA.fromAttempt(Encoder[A].encode(a))

  def encodeBytes[A: Encoder](a: A): Step[ByteVector] =
    encode(a).map(_.toByteVector)

  def log[A](message: A): Step[Unit] =
    ConnectionA.liftF(ConnectionA.Log(message.toString))

  def unit: Step[Unit] =
    Free.pure(())

  object Effect
  {
    def pure[A](a: A): Effect[A] =
      EitherT.liftF(StateT.liftF(Pull.pure(a)))

    def unit: Effect[Unit] =
      pure(())

    def pull[A](p: Pull[A]): Effect[A] =
      EitherT.liftF(StateT.liftF(p))

    def eval[A](fa: IO[A]): Effect[A] =
      pull(fs2.Pull.eval(fa))

    def either[A](a: Either[Err, A]): Effect[A] =
      EitherT.fromEither[State](a)

    def error(message: String): Effect[Unit] =
      pull(rabid.Log.pull.error[IO]("connection", message))
  }

  object State
  {
    def pure[A](a: A): State[A] =
      StateT.liftF(Pull.pure(a))

    def inspect[A](f: ConnectionResources => A): State[A] =
      StateT.inspect(f)

    def modify(f: ConnectionResources => ConnectionResources): State[Unit] =
      StateT.modify(f)

    def pull[A](p: Pull[A]): State[A] =
      StateT.liftF(p)

    def eval[A](fa: IO[A]): State[A] =
      pull(fs2.Pull.eval(fa))
  }
}
