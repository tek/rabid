package rabid
package channel

import fs2.async.mutable.Signal
import cats.implicits._
import cats.effect.IO
import io.circe.Encoder
import io.circe.syntax._

import connection.Input
import Actions._

object programs
{
  def connect: ChannelA.Internal =
    for {
      _ <- sendAmqpHeader
      start <- receiveMethod[Method.connection.Start]
      _ <- sendMethod(method.connection.startOk(start.serverProperties))
      tune <- receiveMethod[Method.connection.Tune]
      _ <- sendMethod(method.connection.tuneOk(tune))
      _ <- sendMethod(method.connection.open)
      openOk <- receiveMethod[Method.connection.OpenOk]
      _ <- Actions.connectionOutput(Input.Connected)
    } yield PNext.Debuffer

  def serverClose(code: Short, text: String, classId: Short, methodId: Short): ChannelA.Internal =
    for {
      _ <- log(s"server closed the connection after method $classId/$methodId with $code: $text")
    } yield PNext.Exit

  def controlListen: ChannelA.Internal =
    for {
      method <- receiveFramePayload[Method]
      _ <- log(s"control received $method")
      output <- method match {
        case Method.connection.Close(code, text, c, m) =>
          serverClose(code, text.data, c, m)
        case _ => ChannelA.pure(PNext.Regular)
      }
    } yield output

  def createChannel(number: Short): ChannelA.Internal =
    for {
      _ <- log(s"creating channel $number")
      _ <- sendMethod(method.channel.open)
      openOk <- receiveMethod[Method.channel.OpenOk]
      _ <- channelOpened
      _ <- connectionOutput(Input.ChannelOpened(number, openOk.channelId.data))
    } yield PNext.Regular

  def declareExchange(name: String): ChannelA.Internal =
    for {
      _ <- log(s"declaring exchange `$name`")
      _ <- sendMethod(method.exchange.declare(name))
      _ <- receiveMethod[Method.exchange.DeclareOk.type]
    } yield PNext.Regular

  def declareQueue(name: String): ChannelA.Internal =
    for {
      _ <- log(s"declaring queue `$name`")
      _ <- sendMethod(method.queue.declare(name))
      _ <- receiveMethod[Method.queue.DeclareOk]
    } yield PNext.Regular

  def bindQueue(exchange: String, queue: String, routingKey: String): ChannelA.Internal =
    for {
      _ <- log(s"binding queue `$queue` to `$exchange:$routingKey`")
      _ <- sendMethod(method.queue.bind(exchange, queue, routingKey))
      _ <- receiveMethod[Method.queue.BindOk.type]
    } yield PNext.Regular

  def publish1(exchange: String, routingKey: String, data: String): ChannelA.Internal =
    for {
      _ <- log(s"publishing to `$exchange:$routingKey`")
      _ <- sendMethod(method.basic.publish(exchange, routingKey))
      _ <- sendContent(ClassId.basic.id, data)
    } yield PNext.Regular

  def publish1Json[A: Encoder](exchange: String, routingKey: String)(message: A): ChannelA.Internal =
    publish1(exchange, routingKey, message.asJson.spaces2)

  def publishJson[A: Encoder](exchange: String, routingKey: String)(messages: List[A]): ChannelA.Internal =
    messages.traverse(publish1Json(exchange, routingKey)).as(PNext.Regular)

  def consume1(queue: String, signal: Signal[IO, Option[Either[String, String]]]): ChannelA.Internal =
    for {
      _ <- log(s"consuming one from `$queue`")
      _ <- sendMethod(method.basic.get(queue, false))
      response <- receiveFramePayload[Method.basic.GetResponse]
       message <- response match {
         case Method.basic.GetResponse(Right(_)) =>
           receiveStringContent.map(Right(_))
         case Method.basic.GetResponse(Left(_)) =>
           ChannelA.pure(Left("no message available"))
       }
       _ <- notifyConsumer(signal, message)
    } yield PNext.Regular

  def syncProg(prog: ChannelA.Step[ChannelOutput]): ChannelA.Internal =
    for {
      a <- prog
      _ <- Actions.output(a)
    } yield PNext.Regular

  def deliver1: ChannelA.Step[Unit] =
    for {
      deliver <- receiveMethod[Method.basic.Deliver]
      data <- receiveStringContent
      _ <- Actions.output(ChannelOutput(Delivery(data, deliver.deliveryTag)))
    } yield ()

  def deliverLoop(stop: Signal[IO, Boolean]): ChannelA.Internal =
    for {
      _ <- deliver1
      a <- deliverLoop(stop)
    } yield a

  def consume(queue: String, stop: Signal[IO, Boolean], ack: Boolean): ChannelA.Internal =
    for {
      _ <- log(s"consuming from `$queue`")
      _ <- sendMethod(method.basic.consume(queue, "", ack))
      _ <- receiveMethod[Method.basic.ConsumeOk]
      next <- deliverLoop(stop)
    } yield next

  def ack1[A](message: Message[A]): ChannelA.Internal =
    sendMethod(method.basic.ack(message.deliveryTag, false)).as(PNext.Regular)

  def ack[A](messages: List[Message[A]]): ChannelA.Internal =
    for {
      _ <- log(s"acking `$messages`")
      _ <- messages.traverse(ack1)
    } yield PNext.Regular
}
