package rabid

import cats.effect.IO
import scodec.{Attempt, Decoder, Err}
import scodec.bits.{BitVector, ByteVector}

import Field._

class DecodeTableSpec
extends Test
{
  def data = "0c6361706162696c697469657346000000c7127075626c69736865725f636f6e6669726d7374011a65786368616e67655f65786368616e67655f62696e64696e677374010a62617369632e6e61636b740116636f6e73756d65725f63616e63656c5f6e6f74696679740112636f6e6e656374696f6e2e626c6f636b6564740113636f6e73756d65725f7072696f72697469657374011c61757468656e7469636174696f6e5f6661696c7572655f636c6f73657401107065725f636f6e73756d65725f716f7374010f6469726563745f7265706c795f746f74010c636c75737465725f6e616d6553000000137261626269744039656535623633333163316409636f70797269676874530000002e436f707972696768742028432920323030372d32303138205069766f74616c20536f6674776172652c20496e632e0b696e666f726d6174696f6e53000000354c6963656e73656420756e64657220746865204d504c2e202053656520687474703a2f2f7777772e7261626269746d712e636f6d2f08706c6174666f726d530000001145726c616e672f4f54502032302e332e340770726f6475637453000000085261626269744d510776657273696f6e5300000005332e372e34"

  def target = List(
    TableItem("publisher_confirms", Bool(true)),
    TableItem("exchange_exchange_bindings", Bool(true)),
    TableItem("basic.nack", Bool(true)),
    TableItem("consumer_cancel_notify", Bool(true)),
    TableItem("connection.blocked", Bool(true)),
    TableItem("consumer_priorities", Bool(true)),
    TableItem("authentication_failure_close", Bool(true)),
    TableItem("per_consumer_qos", Bool(true)),
    TableItem("direct_reply_to", Bool(true)),
 )

  def hex2char(a: String): Option[Char] = ByteVector.fromHex(a).map(_.toByte(true).toChar)

  def decode =
    for {
      bits <- Attempt.fromOption(BitVector.fromHex(data), Err.General("couldn't parse hex data", Nil))
      a <- Decoder[Field.ShortString].decode(bits)
      result <- Decoder[Typed].decode(a.remainder)
    } yield result.value

  test("decode a table recorded from rabbit in hex") {
    IO.pure(decode) must_== Attempt.Successful(Typed('F', Table(target)))
  }
}
