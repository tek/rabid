package rabid

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import fs2.Stream
import cats.implicits._
import cats.effect.IO
import _root_.io.circe.generic.auto._
import org.specs2.Specification

import connection.Connection
import channel.{ExchangeConf, QueueConf, Delivery, DeliveryTag}
import ConsumeEC.ec

case class Data(num: Int)

case class DataError(data: String, tag: DeliveryTag, message: String)

object ConsumeEC
{
  implicit val ec: ExecutionContext = EC(20)
}

object Consumer
{
  val exchange = ExchangeConf("ex", "topic", false)

  def apply(): RabidStream[Unit] =
    for {
      pubChannel <- RabidStream.liftIO(rabid.openChannel)
      _ <- RabidStream.liftF(
        rabid.publishJsonIn(exchange, "root")(List(Data(1), Data(2), Data(3), Data(4))).apply(pubChannel)
      )
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

object ConsumeSpec
{
  val conf = RabidConf(ServerConf("localhost", 5672), ConnectionConf("test", "test", "/test"), QosConf(0, 100.toShort))

  def apply(): Stream[IO, Unit] =
    for {
      connection <- Connection.native(conf)
      _ <- StreamUtil.timed(2.seconds)(Rabid.run(Consumer())(connection))
    } yield ()
}

class ConsumeSpec
extends Specification
{
  def is = s2"""
  consume $consume
  """

  def consume = {
    ConsumeSpec().compile.drain.unsafeRunSync()
    1 === 1
  }
}
