package rabid

import java.nio.channels.AsynchronousChannelGroup

import scala.concurrent.ExecutionContext

import fs2.Stream
import fs2.async.mutable.{Queue => FQueue, Signal}
import cats.effect.IO
import cats.implicits._
import io.circe.{Encoder, Decoder}
import io.circe.syntax._
import io.circe.parser._

import connection.{Connection, Input}
import channel.{Channel, ChannelInput, programs}

// in order to remove the Signal from the consume programs, make ChannelInput an ADT with variants Sync/Queue/Signal
// handle the signal or queue to the channel interpreter depending on the data type
object Api
{
  def send(name: String, thunk: Channel.Prog)(channel: Channel): Stream[IO, Unit] =
    Stream.eval(channel.exchange.in.enqueue1(ChannelInput.Prog(name, thunk)))

  def declareExchange(name: String): Channel => Stream[IO, Unit] =
    send(s"declare exchange `$name`", programs.declareExchange(name))

  def bindQueue(exchange: String, queue: String, routingKey: String): Channel => Stream[IO, Unit] =
    send(s"bind queue `$queue`", programs.bindQueue(exchange, queue, routingKey))
}

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
      data <- signal.discrete.collect { case Some(a) => a }.take(1)
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
{
  def consume[A: Decoder]: Stream[IO, Stream[IO, A]] = ???
}

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

object Rabid
{
  def native
  (host: String, port: Int)
  (consume: Rabid => Stream[IO, Unit])
  (implicit ec: ExecutionContext, ag: AsynchronousChannelGroup)
  : Stream[IO, Unit] =
    for {
      (api, main, close) <- Connection.native(host, port)
      _ <- main.concurrently(consume(api))
      _ <- Stream.eval(close)
    } yield ()
}
