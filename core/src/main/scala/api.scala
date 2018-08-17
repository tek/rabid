package rabid

import java.nio.channels.AsynchronousChannelGroup

import scala.concurrent.ExecutionContext

import fs2.Stream
import fs2.async.mutable.Queue
import cats.effect.IO
import io.circe.Encoder
import io.circe.syntax._

import connection.{ConsumerRequest, Connection}
import channel.{Channel, ChannelProg, programs}

object Api
{
  def send(name: String, thunk: Channel.Thunk)(channel: Channel)
  : Stream[IO, Unit] =
    Stream.eval(channel.exchange.in.enqueue1(ChannelProg(name, thunk)))

    def declareExchange(name: String): Channel => Stream[IO, Unit] =
    send(s"declare exchange `$name`", programs.declareExchange(name))
}

case class ExchangeApi(name: String, channel: Channel)
{
  def publish1[A: Encoder](routingKey: String)(message: A): Stream[IO, Unit] =
    Api.send(
      s"publish to `$name` as `$routingKey`: $message",
      programs.publish1(name, routingKey, message.asJson.spaces2)
    )(channel)
}

case class QueueApi(exchange: ExchangeApi, name: String, channel: Channel)

case class ChannelApi(channel: Channel)
{
  def exchange(name: String): Stream[IO, ExchangeApi] =
    for {
      _ <- Api.send(s"declare exchange `$name`", programs.declareExchange(name))(channel)
    } yield ExchangeApi(name, channel)

  def simpleQueue(name: String): Stream[IO, QueueApi] =
    for {
      ex <- exchange(name)
      _ <- Api.send(s"declare queue `$name`", programs.declareQueue(name))(channel)
      _ <- Api.send(s"bind queue `$name`", programs.bindQueue(name, name, name))(channel)
    } yield QueueApi(ex, name, channel)
}

case class Rabid(queue: Queue[IO, ConsumerRequest])
{
  def channel(implicit ec: ExecutionContext): Stream[IO, ChannelApi] =
    Stream.eval(
      for {
        channel <- Channel.cons
        _ <- queue.enqueue1(ConsumerRequest.CreateChannel(channel))
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
