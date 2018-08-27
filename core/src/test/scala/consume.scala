package rabid

import java.nio.channels.AsynchronousChannelGroup
import java.nio.channels.spi.AsynchronousChannelProvider

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import fs2.Stream
import fs2.internal.ThreadFactories
import cats.implicits._
import cats.effect.IO
import cats.free.Free
import _root_.io.circe.generic.auto._
import org.specs2.Specification

sealed trait ApiA[A]

object ApiA
{
  case object Channel
  extends ApiA[Unit]

  case class Exchange(name: String)
  extends ApiA[Unit]

  case class Queue(name: String)
  extends ApiA[Unit]

  case class BoundQueue(exchange: String, queue: String, routingKey: String)
  extends ApiA[Unit]
}

object free
{
  def channel: Free[ApiA, Unit] =
    Free.liftF(ApiA.Channel)
}

case class Data(num: Int)

object Consume
{
  def apply(): RabidStream[Unit] =
    for {
      _ <- rabid.publishJson("ex", "root")(List(Data(1), Data(2), Data(3), Data(4)))
      (ack, messages) <- rabid.consumeJson[Data]("ex", "cue", "root", true)
      a <- RabidStream.liftF(messages)
      _ <- RabidStream.eval(ack(List(a)))
    } yield ()
}

object ConsumeSpec
{
  implicit val tcpACG: AsynchronousChannelGroup =
    AsynchronousChannelProvider
      .provider()
      .openAsynchronousChannelGroup(1, ThreadFactories.named("rabbit", true))

  def connect: Stream[IO, Unit] =
    StreamUtil.timed(2.seconds)(Rabid.native("localhost", 5672)(Consume()))
}

class ConsumeSpec
extends Specification
{
  def is = s2"""
  connect $connect
  """

  def connect = {
    ConsumeSpec.connect.compile.drain.unsafeRunSync()
    1 === 1
  }
}

class TestSpec
extends Specification
{
  def is = s2"""
  test $test
  """

  def test = {
    1 === 1
  }
}
