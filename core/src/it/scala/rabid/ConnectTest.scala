package rabid

import cats.effect.{ContextShift, IO}
import fs2.Stream
import rabid.connection.Connection

object ConnectTest
{
  val conf = RabidConf(ServerConf("localhost", 5672), ConnectionConf("test", "test", "/test"), QosConf(0, 100.toShort))

  def run(implicit cs: ContextShift[IO]): Stream[IO, Boolean] =
    Stream.resource(Connection.native(conf)).as(true)
}

class ConnectTest
extends Test
{
  test("connect")(ConnectTest.run.compile.lastOrError)
}
