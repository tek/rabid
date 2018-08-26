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
import channel.{Channel, ChannelA, ChannelInput, ChannelOutput, programs, ChannelInterrupt}

case class Message[A](data: A, deliveryTag: Long)

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

object Rabid
{
  def native[A]
  (host: String, port: Int)
  (consume: Rabid => Stream[IO, A])
  (implicit ec: ExecutionContext, ag: AsynchronousChannelGroup)
  : Stream[IO, A] =
    for {
      (api, main) <- Connection.native(host, port)
      a <- consume(api).concurrently(main)
    } yield a

  def openChannel(rabid: Rabid)(implicit ec: ExecutionContext): IO[Channel] =
    for {
      channel <- Channel.cons
      _ <- rabid.queue.enqueue1(Input.OpenChannel(channel))
    } yield channel

  def sendToChannel(channel: Channel)(name: String, prog: ChannelA.Internal): IO[Unit] =
      channel.exchange.in.enqueue1(ChannelInput.Prog(name, prog))

  def unitChannel(channel: Channel)(name: String, prog: ChannelA.Step[Unit]): IO[Unit] =
    sendToChannel(channel)(name, prog.as(PNext.Regular))

  def consumeChannel(channel: Channel): Stream[IO, ChannelOutput] =
    channel.exchange.out.dequeue

  def consumerChannel(channel: Channel)(name: String, prog: ChannelA.Step[Unit]): Stream[IO, ChannelOutput] =
    Stream.eval(unitChannel(channel)(name, prog)) >> consumeChannel(channel)

  def syncProg[A](prog: ChannelA.Step[A], comm: FQueue[IO, A]): ChannelA.Step[Unit] =
    for {
      a <- prog
      _ <- channel.Actions.eval(comm.enqueue1(a))
    } yield ()

  def syncChannel[A](channel: Channel)(name: String, prog: ChannelA.Step[A])
  (implicit ec: ExecutionContext)
  : IO[A] =
    for {
      comm <- FQueue.bounded[IO, A](1)
      _ <- unitChannel(channel)(name, syncProg(prog, comm))
      a <- comm.dequeue1
    } yield a

  def publish1[A: Encoder](channel: Channel, exchange: String, routingKey: String)(message: A): IO[Unit] =
    sendToChannel(channel)(
      s"publish to `$exchange` as `$routingKey`: $message",
      programs.publish1(exchange, routingKey, message.asJson.spaces2),
    )

  def publish[A: Encoder](rabid: Rabid)(exchange: String, routingKey: String)(messages: List[A])
  (implicit ec: ExecutionContext)
  : IO[Unit] =
    for {
      channel <- openChannel(rabid)
      _ <- messages.traverse(publish1(channel, exchange, routingKey)).void
    } yield ()

  def consumeProg(stop: Signal[IO, Boolean])
  (exchange: String, queue: String, route: String, ack: Boolean)
  : ChannelA.Step[Unit] =
    for {
      _ <- programs.declareExchange(exchange)
      _ <- programs.declareQueue(queue)
      _ <- programs.bindQueue(exchange, queue, route)
      _ <- programs.consume(queue, stop, ack)
    } yield ()

  def interruptChannel(channel: Channel)(message: ChannelInterrupt): IO[Unit] =
    channel.receive.enqueue1(Left(message))

  def acker[A](channel: Channel): List[Message[A]] => IO[Unit] =
    messages => messages.map(a => ChannelInterrupt.Ack(a.deliveryTag, false)).traverse(interruptChannel(channel)).void

  def consumeJsonIn[A: Decoder]
  (channel: Channel)
  (stop: Signal[IO, Boolean])
  (exchange: String, queue: String, route: String, ack: Boolean)
  : Stream[IO, Message[A]] =
    for {
      _ <- consumerChannel(channel)(
        s"consume json from $exchange:$queue:$route", consumeProg(stop)(exchange, queue, route, ack))
      output <- consumeChannel(channel)
      data <- decode[A](output.message.data) match {
        case Right(a) => Stream(Message(a, output.message.deliveryTag))
        case Left(error) =>
          println(error)
          Stream.empty
      }
    } yield data

  def consumeJson[A: Decoder](rabid: Rabid)(exchange: String, queue: String, route: String, ack: Boolean)
  (implicit ec: ExecutionContext)
  : Stream[IO, (List[Message[A]] => IO[Unit], Stream[IO, Message[A]])] =
    for {
      stop <- Signals.event
      channel <- Stream.eval(openChannel(rabid))
    } yield (acker(channel), consumeJsonIn[A](channel)(stop)(exchange, queue, route, ack))
}
