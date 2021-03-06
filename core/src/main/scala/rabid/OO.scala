package rabid
package oo

import _root_.io.circe.{Decoder, Encoder}
import _root_.io.circe.parser._
import _root_.io.circe.syntax._
import cats.effect.{ContextShift, IO}
import cats.implicits._
import channel.{Channel, ExchangeConf, QueueConf, programs}
import connection.Input
import fs2.Stream
import fs2.concurrent.{Queue => FQueue, SignallingRef}

case class Queue(name: String, channel: Channel)
{
  def bind(exchange: Exchange, routingKey: String): Stream[IO, BoundQueue] =
    Api.bindQueue(exchange.name, name, routingKey)(channel).as(BoundQueue(exchange, this, routingKey, channel))

  def consume1[A: Decoder]
  (implicit cs: ContextShift[IO])
  : Stream[IO, Either[String, A]] =
    for {
      signal <- Stream.eval(SignallingRef[IO, Option[Either[String, String]]](None))
       _ <- Api.send(
         s"consume one from `$name`",
         programs.consume1(name, signal),
       )(channel)
      data <- signal.discrete.unNone.head
    } yield data.flatMap(decode[A]).leftMap(_.toString)
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
  def exchange(exchange: ExchangeConf): Stream[IO, Exchange] =
    for {
      _ <- Api.declareExchange(exchange)(channel)
    } yield Exchange(exchange.name, channel)

  def queue(conf: QueueConf): Stream[IO, Queue] =
    Api.send(s"declare queue `${conf.name}`", programs.declareQueue(conf))(channel).as(Queue(conf.name, channel))

  def boundQueue(exchangeConf: ExchangeConf, queueConf: QueueConf, routingKey: String): Stream[IO, BoundQueue] =
    for {
      ex <- exchange(exchangeConf)
      q <- queue(queueConf)
      _ <- Api.bindQueue(exchangeConf.name, queueConf.name, routingKey)(channel)
    } yield BoundQueue(ex, q, routingKey, channel)

  def simpleQueue(name: String): Stream[IO, BoundQueue] =
    boundQueue(ExchangeConf(name, "topic", true), QueueConf(name, true), name)
}

case class Rabid(queue: FQueue[IO, Input])
{
  def channel(implicit cs: ContextShift[IO]): Stream[IO, ChannelApi] =
    Stream.eval(
      for {
        channel <- Channel.cons
        _ <- queue.enqueue1(Input.OpenChannel(channel))
      } yield ChannelApi(channel)
    )
}
