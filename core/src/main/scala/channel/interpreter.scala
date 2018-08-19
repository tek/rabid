package rabid
package channel

import fs2.Pull
import fs2.async.mutable.Signal
import scodec.bits.ByteVector
import cats.~>
import cats.data.EitherT
import cats.effect.IO
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

import connection.{Message, Communicate}

object Interpreter
{
  def send(channel: ChannelConnection)(message: Message): Action.Effect[Unit] =
    for {
      _ <- Action.Effect.eval(log(s"channel ${channel.number}", s"sending $message"))
      _ <- Action.Effect.pull(Pull.output1(Communicate.Rabbit(message)))
    } yield ()

  def sendContent(channel: ChannelConnection, classId: Short, payload: ByteVector): Action.Effect[Unit] =
    for {
      frames <- Action.Effect.attempt(Message.Frame.content(channel.number, classId, payload))
      _ <- frames.traverse(send(channel))
    } yield ()

  def receive(channel: ChannelConnection): Action.Effect[ByteVector] =
    EitherT.liftF(Pull.eval(channel.receive.dequeue1))

  def receiveContent(channel: ChannelConnection): Action.Effect[ByteVector] =
    for {
      header <- receive(channel)
      body <- receive(channel)
    } yield body

  def notifyConsumer(signal: Signal[IO, Option[Either[String, String]]], data: Either[String, String])
  : Action.Effect[Unit] =
    Action.Effect.eval(signal.set(Some(data)))

  def log(name: String, message: String): IO[Unit] =
    for {
      logger <- Slf4jLogger.fromName[IO](name)
      _ <- logger.info(message)
    } yield ()

  def awaitSignal(signal: Signal[IO, Boolean]): Action.Effect[Unit] =
    Action.Effect.eval(signal.discrete.filter(identity).take(1).compile.drain)

  def interpreter(channel: ChannelConnection): Action ~> Action.Effect =
    new (Action ~> Action.Effect) {
      def apply[A](action: Action[A]): Action.Effect[A] =
        action match {
          case Action.SendAmqpHeader =>
            send(channel)(Message.header)
          case Action.SendMethod(payload) =>
            send(channel)(Message.Frame.method(channel.number, payload))
          case Action.SendContent(classId, payload) =>
            sendContent(channel, classId, payload)
          case Action.Receive =>
            receive(channel)
          case Action.ReceiveContent =>
            receiveContent(channel)
          case Action.NotifyConsumer(signal, data) =>
            notifyConsumer(signal, data)
          case Action.Log(message) =>
            Action.Effect.eval(log(s"channel ${channel.number}", message))
          case Action.Output(comm) =>
            Action.Effect.pull(Pull.output1(comm))
          case Action.AwaitConnection =>
            awaitSignal(channel.connected)
          case Action.AwaitChannel =>
            awaitSignal(channel.created)
          case Action.ChannelCreated =>
            Action.Effect.eval(channel.created.set(true))
        }
    }
}
