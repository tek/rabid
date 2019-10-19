package rabid

import cats.effect.IO
import scodec.{Attempt, Decoder, Encoder}
import xpct.Xp

object CodecSpec
extends Test
{
  def code[A: Encoder: Decoder](a: A): Attempt[A] =
    for {
      bits <- Encoder.encode(a)
      back <- Decoder[A].decode(bits)
    } yield back.value

  def checkCode[A: Encoder: Decoder](a: A): Xp[IO, Attempt[A]] =
    IO.pure(code(a)) must_== Attempt.Successful(a)
}
