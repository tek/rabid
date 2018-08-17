package rabid

import scodec.{Encoder, Decoder, Attempt}
import org.specs2.matcher.MustMatchers._
import org.specs2.matcher.MatchResult

object CodecSpec
{
  def code[A: Encoder: Decoder](a: A): Attempt[A] =
    for {
      bits <- Encoder.encode(a)
      back <- Decoder[A].decode(bits)
    } yield back.value

  def checkCode[A: Encoder: Decoder](a: A): MatchResult[Attempt[A]] =
    code(a) must be_===(Attempt.Successful(a))
}
