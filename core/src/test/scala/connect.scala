package rabid

import java.nio.channels.AsynchronousChannelGroup
import java.nio.channels.spi.AsynchronousChannelProvider

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import fs2.{Stream, Scheduler}
import fs2.async.mutable.Signal
import fs2.internal.ThreadFactories
import cats.effect.IO
import cats.free.Free
import cats.implicits._
import io.circe.generic.auto._
import org.specs2.Specification

import channel.{programs, ChannelA}

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

object ConnectSpec
{
  implicit val tcpACG: AsynchronousChannelGroup = AsynchronousChannelProvider
    .provider()
    .openAsynchronousChannelGroup(20, ThreadFactories.named("rabbit", true))

  def consumeSingle(rabid: Rabid): Stream[IO, Unit] =
    for {
      channel <- rabid.channel
      queue <- channel.simpleQueue("cue")
      _ <- queue.exchange.publish1("cue")(Data(1))
      _ <- queue.exchange.publish1("cue")(Data(2))
      _ <- queue.exchange.publish("cue")(List(Data(3), Data(4)))
      a <- queue.queue.consume1[Data]
      b <- queue.queue.consume1[Data]
    } yield {
      println(b)
    }

  def consume1(name: String, route: String, stop: Signal[IO, Boolean]): ChannelA.Step[Unit] =
    for {
      _ <- programs.declareExchange(name)
      _ <- programs.publishJson(name, route)(List(Data(1), Data(2), Data(3), Data(4)))
      queue <- programs.declareQueue(name)
      bound <- programs.bindQueue(name, name, route)
      _ <- programs.consume(name, stop, false)
    } yield ()

  def timed[A](s: Stream[IO, A]): Stream[IO, A] =
    for {
      scheduler <- Scheduler[IO](1)
      a <- s.mergeHaltR(scheduler.sleep[IO](2.seconds).drain)
    } yield a

  def consume(rabid: Rabid): Stream[IO, Data] =
    for {
      _ <- Stream.eval(Rabid.publish(rabid)("ex", "root")(List(Data(1), Data(2), Data(3), Data(4))))
      (ack, messages) <- Rabid.consumeJson[Data](rabid)("ex", "cue", "root", true)
      a <- messages
      _ <- Stream.eval(ack(List(a)))
    } yield a.data

  def connect: Stream[IO, Unit] =
    for {
      output <- timed(Rabid.native("localhost", 5672)(consume))
    } yield println(output)
}

class ConnectSpec
extends Specification
{
  def is = s2"""
  connect $connect
  """

  def connect = {
    ConnectSpec.connect.compile.drain.unsafeRunSync()
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
