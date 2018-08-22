package rabid
package channel

import fs2.Pull
import fs2.async.mutable.Signal
import scodec.bits.ByteVector
import cats.~>
import cats.effect.IO
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

import connection.{Message, Input}

object Interpreter
{
  def send(channel: ChannelConnection)(message: Message): ChannelA.Effect[Unit] =
    for {
      _ <- ChannelA.Effect.eval(log(s"channel ${channel.number}", s"sending $message"))
      _ <- ChannelA.Effect.pull(Pull.output1(Input.Rabbit(message)))
    } yield ()

  def sendContent(channel: ChannelConnection, classId: Short, payload: ByteVector): ChannelA.Effect[Unit] =
    for {
      frames <- ChannelA.Effect.attempt(Message.Frame.content(channel.number, classId, payload))
      _ <- frames.traverse(send(channel))
    } yield ()

  def receive(channel: ChannelConnection): ChannelA.Effect[ByteVector] =
    ChannelA.Effect.eval(channel.receive.dequeue1)

  def receiveContent(channel: ChannelConnection): ChannelA.Effect[ByteVector] =
    for {
      header <- receive(channel)
      body <- receive(channel)
    } yield body

  def notifyConsumer(signal: Signal[IO, Option[Either[String, String]]], data: Either[String, String])
  : ChannelA.Effect[Unit] =
    ChannelA.Effect.eval(signal.set(Some(data)))

  def log(name: String, message: String): IO[Unit] =
    for {
      logger <- Slf4jLogger.fromName[IO](name)
      _ <- logger.info(message)
    } yield ()

  def awaitSignal(signal: Signal[IO, Boolean]): ChannelA.Effect[Unit] =
    ChannelA.Effect.eval(signal.discrete.filter(identity).take(1).compile.drain)

  def interpreter(channel: ChannelConnection): ChannelA ~> ChannelA.Effect =
    new (ChannelA ~> ChannelA.Effect) {
      def apply[A](action: ChannelA[A]): ChannelA.Effect[A] =
        action match {
          case ChannelA.SendAmqpHeader =>
            send(channel)(Message.header)
          case ChannelA.SendMethod(payload) =>
            send(channel)(Message.Frame.method(channel.number, payload))
          case ChannelA.SendContent(classId, payload) =>
            sendContent(channel, classId, payload)
          case ChannelA.Receive =>
            receive(channel)
          case ChannelA.ReceiveContent =>
            receiveContent(channel)
          case ChannelA.NotifyConsumer(signal, data) =>
            notifyConsumer(signal, data)
          case ChannelA.Log(message) =>
            ChannelA.Effect.eval(log(s"channel ${channel.number}", message))
          case ChannelA.Output(comm) =>
            ChannelA.Effect.pull(Pull.output1(comm))
          case ChannelA.ChannelOpened =>
            ChannelA.Effect.unit
        }
    }
}
