package rabid
package channel

import fs2.Pull
import fs2.async.mutable.Signal
import scodec.bits.ByteVector
import cats.~>
import cats.effect.IO
import cats.implicits._

import connection.{Message, Input}

object Interpreter
{
  def send(channel: ChannelConnection)(message: Message): ChannelA.Effect[Unit] =
    for {
      _ <- ChannelA.Effect.eval(Log.info[IO](s"channel ${channel.number}", s"sending $message"))
      _ <- ChannelA.Effect.pull(Pull.output1(Input.Rabbit(message)))
    } yield ()

  def sendContent(channel: ChannelConnection, classId: Short, payload: ByteVector): ChannelA.Effect[Unit] =
    for {
      frames <- ChannelA.Effect.attempt(Message.Frame.content(channel.number, classId, payload))
      _ <- frames.traverse(send(channel))
    } yield ()

  def interruptProg: ChannelInterrupt => ChannelA.Step[Unit] = {
    case ChannelInterrupt.Ack(deliveryTag, multiple) =>
      Actions.sendMethod(method.basic.ack(deliveryTag, multiple))
  }

  def interrupt(connection: ChannelConnection)(job: ChannelInterrupt): ChannelA.Effect[Unit] =
    interruptProg(job).foldMap(Process.interpretAttempt(interpreter(connection)))

  def receive(connection: ChannelConnection): ChannelA.Effect[ByteVector] =
    for {
      input <- ChannelA.Effect.eval(connection.receive.dequeue1)
      a <- input match {
        case Right(bytes) => ChannelA.Effect.pure(bytes)
        case Left(job) =>
          for {
            _ <- interrupt(connection)(job)
            a <- receive(connection)
          } yield a
      }
    } yield a

  def receiveContent(channel: ChannelConnection): ChannelA.Effect[ByteVector] =
    for {
      header <- receive(channel)
      body <- receive(channel)
    } yield body

  def notifyConsumer(signal: Signal[IO, Option[Either[String, String]]], data: Either[String, String])
  : ChannelA.Effect[Unit] =
    ChannelA.Effect.eval(signal.set(Some(data)))

  def output(connection: ChannelConnection)(data: ChannelOutput): ChannelA.Effect[Unit] =
    ChannelA.Effect.eval(connection.channel.exchange.out.enqueue1(data))

  def awaitSignal(signal: Signal[IO, Boolean]): ChannelA.Effect[Unit] =
    ChannelA.Effect.eval(signal.discrete.filter(identity).take(1).compile.drain)

  def interpreter(connection: ChannelConnection): ChannelA ~> ChannelA.Effect =
    new (ChannelA ~> ChannelA.Effect) {
      def apply[A](action: ChannelA[A]): ChannelA.Effect[A] =
        action match {
          case ChannelA.SendAmqpHeader =>
            send(connection)(Message.header)
          case ChannelA.SendMethod(payload) =>
            send(connection)(Message.Frame.method(connection.number, payload))
          case ChannelA.SendContent(classId, payload) =>
            sendContent(connection, classId, payload)
          case ChannelA.Receive =>
            receive(connection)
          case ChannelA.ReceiveContent =>
            receiveContent(connection)
          case ChannelA.NotifyConsumer(signal, data) =>
            notifyConsumer(signal, data)
          case ChannelA.Log(message) =>
            ChannelA.Effect.eval(Log.info[IO](s"channel ${connection.number}", message))
          case ChannelA.ConnectionOutput(comm) =>
            ChannelA.Effect.pull(Pull.output1(comm))
          case ChannelA.ChannelOpened =>
            ChannelA.Effect.unit
          case ChannelA.Output(data) =>
            output(connection)(data)
          case ChannelA.Eval(fa) =>
            ChannelA.Effect.eval(fa)
        }
    }
}
