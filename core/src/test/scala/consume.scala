package rabid

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import fs2.Stream
import cats.implicits._
import cats.effect.IO
import _root_.io.circe.generic.auto._
import org.specs2.Specification

import connection.Connection
import ConsumeEC.ec

case class Data(num: Int)

object ConsumeEC
{
  implicit val ec: ExecutionContext = EC(20)
}

object Consume
{
  def apply(): RabidStream[Unit] =
    for {
      _ <- rabid.publishJson("ex", "root")(List(Data(1), Data(2), Data(3), Data(4)))
      (ack, messages) <- rabid.consumeJson[Data]("ex", "cue", "root", true)
      a <- RabidStream.liftF(messages)
      _ <- RabidStream.eval(ack(if (a.data.num == 3) Nil else List(a)))
    } yield ()
}

object ConsumeSpec
{
  def apply(): Stream[IO, Unit] =
    for {
      connection <- Connection.native("localhost", 5672)
      _ <- StreamUtil.timed(2.seconds)(Rabid.run(Consume())(connection))
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
