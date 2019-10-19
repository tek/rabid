package rabid

import CodecTest._
import Field._

object CodecTableTest
{
  val table = Table(List(TableItem("key", LongString("value"))))
}

class CodecTableTest
extends Test
{
  test("codec a table")(checkCode(CodecTableTest.table))
}
