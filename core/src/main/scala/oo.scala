package rabid
package oo

import scala.concurrent.ExecutionContext

import fs2.Stream
import fs2.async.mutable.{Queue => FQueue, Signal}
import cats.implicits._
import cats.effect.IO
import io.circe.{Encoder, Decoder}
import io.circe.syntax._
import io.circe.parser._

import connection.Input
import channel.{Channel, programs}

case class Queue(name: String, channel: Channel)
{
  def bind(exchange: Exchange, routingKey: String): Stream[IO, BoundQueue] =
    Api.bindQueue(exchange.name, name, routingKey)(channel).as(BoundQueue(exchange, this, routingKey, channel))

  def consume1[A: Decoder]
  (implicit ec: ExecutionContext)
  : Stream[IO, Either[String, A]] =
    for {
      signal <- Stream.eval(Signal[IO, Option[Either[String, String]]](None))
       _ <- Api.send(
         s"consume one from `$name`",
         programs.consume1(name, signal),
       )(channel)
      data <- signal.discrete.unNone.head
    } yield for {
      message <- data
      a <- decode[A](message).leftMap(_.toString)
    } yield a
}

case class Exchange(name: String, channel: Channel)
{
  def publish1[A: Encoder](routingKey: String)(message: A): Stream[IO, Unit] =
    Api.send(
      s"publish to `$name` as `$routingKey`: $message",
      programs.publish1(name, routingKey, message.asJson.spaces2),
    )(channel)

  def publish[A: Encoder](routingKey: String)(messages: List[A]): Stream[IO, Unit] =
    messages.traverse(publish1(routingKey)).void

  def bind(routingKey: String)(queue: Queue): Stream[IO, BoundQueue] =
    Api.bindQueue(name, queue.name, routingKey)(channel).as(BoundQueue(this, queue, routingKey, channel))
}

case class BoundQueue(exchange: Exchange, queue: Queue, routingKey: String, channel: Channel)

case class ChannelApi(channel: Channel)
{
  def exchange(name: String): Stream[IO, Exchange] =
    for {
      _ <- Api.declareExchange(name)(channel)
    } yield Exchange(name, channel)

  def queue(name: String): Stream[IO, Queue] =
    Api.send(s"declare queue `$name`", programs.declareQueue(name))(channel).as(Queue(name, channel))

  def boundQueue(exchangeName: String, queueName: String, routingKey: String): Stream[IO, BoundQueue] =
    for {
      ex <- exchange(exchangeName)
      q <- queue(queueName)
      _ <- Api.bindQueue(exchangeName, queueName, routingKey)(channel)
    } yield BoundQueue(ex, q, routingKey, channel)

  def simpleQueue(name: String): Stream[IO, BoundQueue] =
    boundQueue(name, name, name)
}

case class Rabid(queue: FQueue[IO, Input])
{
  def channel(implicit ec: ExecutionContext): Stream[IO, ChannelApi] =
    Stream.eval(
      for {
        channel <- Channel.cons
        _ <- queue.enqueue1(Input.OpenChannel(channel))
      } yield ChannelApi(channel)
    )
}
