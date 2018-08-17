package rabid
package channel

import fs2.Pull
import fs2.async.immutable.Signal
import scodec.bits.ByteVector
import cats.~>
import cats.data.EitherT
import cats.effect.IO
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

import connection.{Message, Communicate}

object Interpreter
{
  def send(message: Message): Action.Effect[Unit] =
    for {
      _ <- Action.Effect.eval(log("channel", s"sending $message"))
      _ <- Action.Effect.pull(Pull.output1(Communicate.Rabbit(message)))
    } yield ()

  def receive(channel: ChannelConnection): Action.Effect[ByteVector] =
    EitherT.liftF(Pull.eval(channel.receive.dequeue1))

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
            send(Message.header)
          case Action.SendMethod(payload) =>
            send(Message.Frame.method(channel.number, payload))
          case Action.SendContent(classId, payload) =>
            for {
              frames <- Action.Effect.attempt(Message.Frame.content(channel.number, classId, payload))
              _ <- frames.traverse(send)
            } yield ()
          case Action.Receive =>
            receive(channel)
          case Action.Log(message) =>
            Action.Effect.eval(log(s"channel ${channel.number}", message))
          case Action.Output(comm: Communicate) =>
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
