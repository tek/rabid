package rabid
package channel

import scodec.bits.BitVector
import scodec.codecs.utf8
import scodec.codecs.implicits._
import fs2.async.mutable.Signal
import cats.effect.IO
import cats.implicits._

import Field._
import connection.Input
import Actions._

object programs
{
  def connect: Action.Step[ActionResult] =
    for {
      _ <- sendAmqpHeader
      start <- receiveMethod[Method.connection.Start]
      _ <- sendMethod(method.connection.startOk(start.serverProperties))
      tune <- receiveMethod[Method.connection.Tune]
      _ <- sendMethod(method.connection.tuneOk(tune))
      _ <- sendMethod(method.connection.open)
      openOk <- receiveMethod[Method.connection.OpenOk]
      _ <- Action.liftF(Action.Output(Input.Connected))
    } yield ActionResult.Continue

  def serverClose(code: Short, text: String, classId: Short, methodId: Short): Action.Step[ActionResult] =
    for {
      _ <- log(s"server closed the connection after method $classId/$methodId with $code: $text")
    } yield ActionResult.Done

  def controlListen: Action.Step[ActionResult] =
    for {
      method <- receiveFramePayload[Method]
      _ <- log(s"control received $method")
      output <- method match {
        case Method.connection.Close(code, text, c, m) =>
          serverClose(code, text.data, c, m)
        case _ => Action.pure(ActionResult.Continue)
      }
    } yield output

  def createChannel(number: Short): Action.Step[ActionResult] =
    for {
      _ <- log(s"creating channel $number")
      _ <- sendMethod(method.channel.open)
      openOk <- receiveMethod[Method.channel.OpenOk]
      _ <- channelCreated
      _ <- output(Input.ChannelCreated(number, openOk.channelId.data))
    } yield ActionResult.Continue

  def declareExchange(name: String): Action.Step[ActionResult] =
    for {
      _ <- log(s"declaring exchange `$name`")
      _ <- sendMethod(method.exchange.declare(name))
      _ <- receiveMethod[Method.exchange.DeclareOk.type]
    } yield ActionResult.Continue

  def declareQueue(name: String): Action.Step[ActionResult] =
    for {
      _ <- log(s"declaring queue `$name`")
      _ <- sendMethod(method.queue.declare(name))
      _ <- receiveMethod[Method.queue.DeclareOk]
    } yield ActionResult.Continue

  def bindQueue(exchange: String, name: String, routingKey: String): Action.Step[ActionResult] =
    for {
      _ <- log(s"binding queue `$name` to `$exchange`")
      _ <- sendMethod(method.queue.bind(exchange, name, routingKey))
      _ <- receiveMethod[Method.queue.BindOk.type]
    } yield ActionResult.Continue

  def publish1(exchange: String, routingKey: String, data: String): Action.Step[ActionResult] =
    for {
      _ <- log(s"publishing to `$exchange` with `$routingKey`")
      _ <- sendMethod(method.basic.publish(exchange, routingKey))
      _ <- sendContent(ClassId.basic.id, data)
    } yield ActionResult.Continue

  def consume1(queue: String, signal: Signal[IO, Option[Either[String, String]]]): Action.Step[ActionResult] =
    for {
      _ <- log(s"consuming one from `$queue`")
      _ <- sendMethod(method.basic.get(queue, false))
      response <- receiveFramePayload[Method.basic.GetResponse]
       message <- response match {
         case Method.basic.GetResponse(Right(_)) =>
           receiveContent.map(a => utf8.decode(BitVector(a)).toEither.map(_.value).leftMap(_.toString))
         case Method.basic.GetResponse(Left(_)) =>
           Action.pure(Left("no message available"))
       }
       _ <- notifyConsumer(signal, message)
    } yield ActionResult.Continue
}
