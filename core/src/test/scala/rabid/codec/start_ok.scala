package rabid

import connection._
import scodec.Encoder

import CodecSpec._

object CodecStartOkSpec
{
  import Field._

  val props =
    List(
      TableItem("capabilities",
        Table(List(
          TableItem("publisher_confirms", Bool(true)),
          TableItem("exchange_exchange_bindings", Bool(true)),
          TableItem("basic.nack", Bool(true)),
          TableItem("consumer_cancel_notify", Bool(true)),
          TableItem("connection.blocked", Bool(true)),
          TableItem("consumer_priorities", Bool(true)),
          TableItem("authentication_failure_close", Bool(true)),
          TableItem("per_consumer_qos", Bool(true)),
          TableItem("direct_reply_to", Bool(true))
        ))
      ),
      TableItem("cluster_name", LongString("rabbit@9ee5b6331c1d")),
      TableItem("copyright", LongString("Copyright (C) 2018 tryp")),
      TableItem("information", LongString("MIT")),
      TableItem("platform", LongString("Erlang/OTP 20.3.4")),
      TableItem("product", LongString("RabbitMQ")),
      TableItem("version", LongString("3.7.4")),
    )

  val sok = Method.connection.StartOk(Table(props), ShortString("PLAIN"), LongString("test:test"), ShortString("C"))

  val bytes = Encoder.encode(ClassMethod(sok)).require.toByteVector

  val frame = connection.Message.Frame(FrameType.Method, 0, bytes.size.toInt, bytes, connection.Message.Frame.end)
}

class CodecStartOkSpec
extends Test
{
  test("codec a frame")(checkCode(CodecStartOkSpec.frame))
}
