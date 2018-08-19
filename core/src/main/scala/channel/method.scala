package rabid
package channel

import Field._

object method
{
  object connection
  {
    def startOk(caps: Table): Method.connection.StartOk =
      Method.connection.StartOk(
        caps,
        ShortString("PLAIN"),
        LongString("\u0000test\u0000test"),
        ShortString("en_US.UTF-8"),
      )

    def tuneOk: Method.connection.Tune => Method.connection.TuneOk = {
      case Method.connection.Tune(channelMax, frameMax, heartbeat) =>
        Method.connection.TuneOk(channelMax, frameMax, heartbeat)
    }

    def open: Method.connection.Open =
      Method.connection.Open(ShortString("/test"), ShortString(""), Bool(false))
  }

  object channel
  {
    def open: Method.channel.Open =
      Method.channel.Open(ShortString(""))
  }

  object exchange
  {
    def declare(name: String): Method.exchange.Declare =
      Method.exchange.Declare(
        0,
        ShortString(name),
        ShortString("direct"),
        false,
        false,
        false,
        false,
        false,
        Table.empty,
      )
  }

  object queue
  {
    def declare(name: String): Method.queue.Declare =
      Method.queue.Declare(0, ShortString(name), false, false, false, false, false, Table.empty)

    def bind(queue: String, exchange: String, routingKey: String): Method.queue.Bind =
      Method.queue.Bind(0, ShortString(queue), ShortString(exchange), ShortString(routingKey), false, Table.empty)
  }

  object basic
  {
    def publish(exchange: String, routingKey: String): Method.basic.Publish =
      Method.basic.Publish(0, ShortString(exchange), ShortString(routingKey), false, false)

    def get(queue: String, ack: Boolean): Method.basic.Get =
      Method.basic.Get(0, ShortString(queue), !ack)
  }
}
