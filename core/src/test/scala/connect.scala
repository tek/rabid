package rabid

import java.nio.channels.AsynchronousChannelGroup
import java.nio.channels.spi.AsynchronousChannelProvider

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import fs2.{Stream, Scheduler}
import fs2.internal.ThreadFactories
import cats.effect.IO
import cats.implicits._
import io.circe.generic.auto._
import org.specs2.Specification

case class Data(num: Int)

object ConnectSpec
{
  implicit val tcpACG: AsynchronousChannelGroup = AsynchronousChannelProvider
    .provider()
    .openAsynchronousChannelGroup(20, ThreadFactories.named("rabbit", true))

  def consume(rabid: Rabid): Stream[IO, Unit] =
    for {
      channel <- rabid.channel
      queue <- channel.simpleQueue("cue")
      _ <- queue.exchange.publish1("cue")(Data(1))
      _ <- queue.exchange.publish1("cue")(Data(2))
      _ <- queue.exchange.publish("cue")(List(Data(3), Data(4)))
      // a <- queue.consume1[Data]
    } yield {
      // println(a)
    }

  def connectS: Stream[IO, Unit] =
    for {
      scheduler <- Scheduler[IO](1)
      _ <- Rabid.native("localhost", 5672)(consume).mergeHaltR(scheduler.sleep[IO](5.seconds))
    } yield ()

  def connect: IO[Unit] =
    connectS.compile.drain
}

class ConnectSpec
extends Specification
{
  def is = s2"""
  connect $connect
  """

  def connect = {
    ConnectSpec.connect.unsafeRunSync()
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
