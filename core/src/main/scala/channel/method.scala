package rabid
package channel

import Field._

object method
{
  object connection
  {
    def startOk(user: String, password: String, caps: Table): Method.connection.StartOk =
      Method.connection.StartOk(
        caps,
        ShortString("PLAIN"),
        LongString(s"\u0000${user}\u0000${password}"),
        ShortString("en_US.UTF-8"),
      )

    def tuneOk: Method.connection.Tune => Method.connection.TuneOk = {
      case Method.connection.Tune(channelMax, frameMax, heartbeat) =>
        Method.connection.TuneOk(channelMax, frameMax, heartbeat)
    }

    def open(vhost: String): Method.connection.Open =
      Method.connection.Open(ShortString(vhost), ShortString(""), Bool(false))
  }

  object channel
  {
    def open: Method.channel.Open =
      Method.channel.Open(ShortString(""))
  }

  object exchange
  {
    def declare(conf: ExchangeConf): Method.exchange.Declare =
      Method.exchange.Declare(
        0,
        ShortString(conf.name),
        ShortString(conf.tpe),
        false,
        false,
        false,
        conf.durable,
        false,
        Table.empty,
      )
  }

  object queue
  {
    def declare(conf: QueueConf): Method.queue.Declare =
      Method.queue.Declare(0, ShortString(conf.name), false, false, false, conf.durable, false, Table.empty)

    def bind(exchange: String, queue: String, routingKey: String): Method.queue.Bind =
      Method.queue.Bind(0, ShortString(queue), ShortString(exchange), ShortString(routingKey), false, Table.empty)
  }

  object basic
  {
    def publish(exchange: String, routingKey: String): Method.basic.Publish =
      Method.basic.Publish(0, ShortString(exchange), ShortString(routingKey), false, false)

    def get(queue: String, ack: Boolean): Method.basic.Get =
      Method.basic.Get(0, ShortString(queue), !ack)

    def consume(queue: String, consumerTag: String, ack: Boolean): Method.basic.Consume =
      Method.basic.Consume(0, ShortString(queue), ShortString(consumerTag), false, !ack, false, false, Table.empty)

    def ack(deliveryTag: Long, multiple: Boolean): Method.basic.Ack =
      Method.basic.Ack(deliveryTag, multiple)
  }
}
