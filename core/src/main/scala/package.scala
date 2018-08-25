package rabid

import fs2.Stream
import cats.~>
import cats.effect.IO
import io.circe.Decoder

import connection.ConnectionA

object `package`
{
  def consumeJson[A: Decoder]
  (exchange: String, queue: String, route: String)
  (interpreter: ConnectionA ~> ConnectionA.Effect)
  : Stream[IO, A] =
    ???
}
