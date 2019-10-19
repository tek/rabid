package rabid

import scala.concurrent.ExecutionContext

import cats.effect.{ContextShift, IO}
import klk.SbtResources
import xpct.klk.XpctKlkTest

trait Test
extends XpctKlkTest[IO, SbtResources]
{
  implicit val cs: ContextShift[IO] =
    IO.contextShift(ExecutionContext.global)
}
