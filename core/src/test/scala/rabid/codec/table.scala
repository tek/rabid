package rabid

import CodecSpec._
import Field._

object CodecTableSpec
{
  val table = Table(List(TableItem("key", LongString("value"))))
}

class CodecTableSpec
extends Test
{
  test("codec a table")(checkCode(CodecTableSpec.table))
}
