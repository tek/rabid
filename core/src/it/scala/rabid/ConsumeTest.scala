package rabid

import scala.concurrent.duration._

import _root_.io.circe.generic.auto._
import cats.effect.{ContextShift, IO, Timer}
import channel.{Delivery, DeliveryTag, ExchangeConf, QueueConf}
import connection.Connection
import fs2.Stream

case class Data(num: Int)

case class DataError(data: String, tag: DeliveryTag, message: String)

object Consumer
{
  val exchange = ExchangeConf("ex", "topic", false)

  val num = 1000

  def run(implicit cs: ContextShift[IO], timer: Timer[IO]): RabidStream[Unit] =
    for {
      pubChannel <- RabidStream.liftIO(rabid.openChannel)
      _ <- RabidStream.liftF(
        rabid.publishJsonIn(exchange, "root")(1.to(num).map(Data(_)).toList).apply(pubChannel)
      )
      _ <- RabidStream.liftF(Stream.sleep[IO]((num * 5).milliseconds))
      (ack, messages) <- rabid.consumeJson[Data](exchange, QueueConf("cue", true), "root", true)
      item <- RabidStream.liftF(messages)
      _ <- item match {
        case Consume.Message(_, tag) =>
          RabidStream.eval(ack(List(tag)))
        case Consume.JsonError(Delivery(data, tag), error) =>
          RabidStream.liftF(
            rabid.publishJsonIn(exchange, "root.error")(List(DataError(data, tag, error.toString)))
              .apply(pubChannel)
          )
      }
    } yield ()
}

object ConsumeTest
{
  val conf = RabidConf(ServerConf("localhost", 5672), ConnectionConf("test", "test", "/test"), QosConf(0, 100.toShort))

  def run(implicit cs: ContextShift[IO], timer: Timer[IO]): Stream[IO, Unit] =
    for {
      connection <- Stream.resource(Connection.native(conf))
      _ <- StreamUtil.timed((Consumer.num * 8).milliseconds)(Rabid.run(Consumer.run)(connection))
    } yield ()

  def test: Stream[IO, Unit] =
    Concurrency.fixedPoolEcStreamWith(10)
      .flatMap(ec => run(IO.contextShift(ec), IO.timer(ec)))
}

class ConsumeTest
extends Test
{
  test("consume") {
    ConsumeTest.test.compile.drain.xp
  }
}
