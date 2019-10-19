package rabid

import io.circe.Error
import rabid.channel.{Delivery, DeliveryTag}

sealed trait Consume[A]

object Consume
{
  case class Message[A](data: A, tag: DeliveryTag)
  extends Consume[A]

  case class JsonError[A](delivery: Delivery, error: Error)
  extends Consume[A]
}
