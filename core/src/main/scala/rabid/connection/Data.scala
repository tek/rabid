package rabid
package connection

import scala.annotation.tailrec

import cats.effect.IO
import cats.implicits._
import channel.Channel
import fs2.concurrent.Queue
import scodec.{Attempt, Codec, Decoder, Encoder}
import scodec.bits.ByteVector
import scodec.codecs._
import scodec.codecs.implicits._
import shapeless.{::, HNil}

sealed trait FrameType

object FrameType
{
  case object Method
  extends FrameType
  {
    implicit val Codec_Method: Codec[Method.type] = ByteConstant.as(Method)(1)
  }

  case object Header
  extends FrameType
  {
    implicit val Codec_Header: Codec[Header.type] = ByteConstant.as(Header)(2)
  }

  case object Content
  extends FrameType
  {
    implicit val Codec_Content: Codec[Content.type] = ByteConstant.as(Content)(3)
  }

  case object Heartbeat
  extends FrameType
  {
    implicit val Codec_Heartbeat: Codec[Heartbeat.type] = ByteConstant.as(Heartbeat)(8)
  }

  implicit val Codec_FrameType: Codec[FrameType] = Codec.coproduct[FrameType].choice
}

case class FrameHeader(tpe: FrameType, channel: Short, size: Int)

object FrameHeader
{
  implicit val Codec_FrameHeader: Codec[FrameHeader] =
    (Codec[FrameType] :: short16 :: int32).as[FrameHeader]
}

case class FrameBody(payload: ByteVector, end: Byte)

object FrameBody
{
  def codec(size: Int): Codec[FrameBody] =
    (bytes(size) :: byte).as[FrameBody]
}

sealed trait Message

object Message
{
  val bodyMax: Long = 10000000L

  case class Frame(tpe: FrameType, channel: Short, size: Int, payload: ByteVector, end: Byte)
  extends Message

  object Frame
  {
    def payloadEncoder: Encoder[Int :: ByteVector :: Byte :: HNil] =
      Encoder((a: Int :: ByteVector :: Byte :: HNil) => {
        (int32 :: bytes(a.head) :: byte).encode(a)
      })

    def payloadDecoder: Decoder[Int :: ByteVector :: Byte :: HNil] =
      for {
        length <- int32
        data <- bytes(length)
        b <- byte
      } yield length :: data :: b :: HNil

    def payload: Codec[Int :: ByteVector :: Byte :: HNil] =
      Codec(payloadEncoder, payloadDecoder)

    implicit def Codec_Frame: Codec[Frame] =
      (Codec[FrameType] :: short16 :: payload).as[Frame]

    case class ContentHeader(classId: Short, weight: Short, bodySize: Long, flags: Short)

    implicit def Codec_ContentHeader: Codec[ContentHeader] =
      (short16 :: short16 :: int64 :: short16).as[ContentHeader]

    def end: Byte = 206.toByte

    def frame(tpe: FrameType, channel: Short, payload: ByteVector): Frame =
      Frame(tpe, channel, payload.size.toInt, payload, end)

    def method(channel: Short, payload: ByteVector): Frame =
      frame(FrameType.Method, channel, payload)

    def contentHeaderPayload(classId: Short, size: Long): Attempt[ByteVector] =
      Encoder.encode(ContentHeader(classId, 0, size, 0)).map(_.toByteVector)

    def contentHeader(channel: Short, payload: ByteVector): Frame =
      frame(FrameType.Header, channel, payload)

    def contentBody(channel: Short, payload: ByteVector): Frame =
      frame(FrameType.Content, channel, payload)

    def segmentContent(data: ByteVector): List[ByteVector] = {
      @tailrec
      def loop(remainder: ByteVector, segments: List[ByteVector]): List[ByteVector] =
        if (remainder.isEmpty) segments
        else {
          val (segment, rest) = remainder.splitAt(bodyMax)
          loop(rest, segment :: segments)
        }
      loop(data, Nil).reverse
    }

    def content(channel: Short, classId: Short, payload: ByteVector): Attempt[List[Frame]] = {
      for {
        header <- contentHeaderPayload(classId, payload.size)
      } yield contentHeader(channel, header) :: segmentContent(payload).map(a => contentBody(channel, a))
    }
  }

  case class AMQPHeader(data: ByteVector, v1: Int, v2: Int, v3: Int, v4: Int)
  extends Message

  object AMQPHeader
  {
    implicit val Codec_AMQPHeader: Codec[AMQPHeader] =
      (bytes(4) :: int8 :: int8 :: int8 :: int8).as[AMQPHeader]
  }

  def header: AMQPHeader = AMQPHeader(ByteVector('A', 'M', 'Q', 'P'), 0, 0, 9, 1)

  def heartbeat: Frame = Frame.frame(FrameType.Heartbeat, 0, ByteVector.empty)

  implicit val Codec_Message: Codec[Message] =
    Codec.coproduct[Message].choice
}

case class Exchange[A, B](in: Queue[IO, A], out: Queue[IO, B])

sealed trait Input

object Input
{
  case object Connected
  extends Input

  case class Rabbit(message: Message)
  extends Input

  case class ChannelReceive(header: FrameHeader, body: FrameBody)
  extends Input

  case class OpenChannel(channel: Channel)
  extends Input

  case class ChannelOpened(number: Short, id: String)
  extends Input
}

sealed trait ConnectionState

object ConnectionState
{
  case object Disconnected
  extends ConnectionState

  case object Connecting
  extends ConnectionState

  case object Connected
  extends ConnectionState
}

sealed trait ActionResult

object ActionResult
{
  case object Continue
  extends ActionResult

  case object Connected
  extends ActionResult

  case object Running
  extends ActionResult

  case object Done
  extends ActionResult

  case object Started
  extends ActionResult
}

sealed trait Continuation

object Continuation
{
  case object Regular
  extends Continuation

  case object Debuffer
  extends Continuation

  case object Exit
  extends Continuation
}
