package rabid

import org.specs2.Specification

import Field._
import CodecSpec._

object CodecTableSpec
{
  val table = Table(List(TableItem("key", LongString("value"))))
}

class CodecTableSpec
extends Specification
{
  def is = s2"""
  codec a table $codecTable
  """

  def codecTable = checkCode(CodecTableSpec.table)
}
