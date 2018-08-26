package rabid
package channel

import fs2.async.mutable.Queue
import scodec.bits.ByteVector
import cats.effect.IO

case class ChannelOutput(message: Delivery)

sealed trait ChannelInput

object ChannelInput
{
  case class Internal(name: String, prog: ChannelA.Internal)
  extends ChannelInput

  case class Prog(name: String, thunk: Channel.Prog)
  extends ChannelInput

  case class Sync(name: String, thunk: ChannelA.Step[ChannelOutput])
  extends ChannelInput

  case object StopConsumer
  extends ChannelInput

  case object Opened
  extends ChannelInput
}

case class ChannelConnection(
  number: Short,
  channel: Channel,
  receive: Queue[IO, Either[ChannelInterrupt, ByteVector]],
)

case class ChannelData(number: Short, state: PState, buffer: Vector[ChannelInput])

object ChannelData
{
  def cons(number: Short, state: PState): ChannelData =
    ChannelData(number, state, Vector.empty)
}

case class Delivery(data: String, deliveryTag: Long)

sealed trait ChannelInterrupt

object ChannelInterrupt
{
  case class Ack(deliveryTag: Long, multiple: Boolean)
  extends ChannelInterrupt
}
