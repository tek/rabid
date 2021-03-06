package rabid
package channel

import cats.effect.IO
import fs2.concurrent.Queue
import scodec.bits.ByteVector

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
  receive: Queue[IO, ChannelMessage],
  qos: QosConf,
)

case class ChannelData(number: Short, state: PState, buffer: Vector[ChannelInput])

object ChannelData
{
  def cons(number: Short, state: PState): ChannelData =
    ChannelData(number, state, Vector.empty)
}

case class DeliveryTag(data: Long)

case class Delivery(data: String, tag: DeliveryTag)

sealed trait ChannelMessage

object ChannelMessage
{
  case class Rabbit(payload: ByteVector)
  extends ChannelMessage

  case class Ack(tag: Long, multiple: Boolean)
  extends ChannelMessage
}

case class ExchangeConf(name: String, tpe: String, durable: Boolean)

case class QueueConf(name: String, durable: Boolean)
