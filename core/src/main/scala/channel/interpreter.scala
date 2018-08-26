package rabid
package channel

import fs2.Pull
import fs2.async.mutable.Signal
import scodec.bits.ByteVector
import cats.~>
import cats.effect.IO
import cats.implicits._

import connection.{Message, Input}
import ChannelA.{Effect, AKleisli}

object Interpreter
{
  def log(message: String): AKleisli[Unit] =
    AKleisli(c => Effect.eval(Log.info[IO](s"channel ${c.number}", message)))

  def channelNumber: AKleisli[Short] =
    AKleisli.inspect(_.number)

  def send(message: Message): AKleisli[Unit] =
    for {
      _ <- log(s"sending $message")
      _ <- AKleisli.pull(Pull.output1(Input.Rabbit(message)))
    } yield ()

  def sendMethod(payload: ByteVector): AKleisli[Unit] =
    for {
      number <- channelNumber
      _ <- send(Message.Frame.method(number, payload))
    } yield ()

  def sendContent(classId: Short, payload: ByteVector): AKleisli[Unit] =
    for {
      frames <- AKleisli(c => Effect.attempt(Message.Frame.content(c.number, classId, payload)))
      _ <- frames.traverse(send)
    } yield ()

  def ack(deliveryTag: Long, multiple: Boolean): AKleisli[Unit] =
    for {
      interpret <- AKleisli(c => Effect.pure(Process.interpretAttempt(interpreter(c))))
      _ <- AKleisli.liftF(Actions.sendMethod(method.basic.ack(deliveryTag, multiple)).foldMap(interpret))
    } yield ()

  def receive: AKleisli[ByteVector] =
    for {
      input <- AKleisli.applyEval(_.receive.dequeue1)
      a <- input match {
        case ChannelMessage.Rabbit(bytes) => AKleisli.pure(bytes)
        case ChannelMessage.Ack(deliveryTag, multiple) =>
          for {
            _ <- ack(deliveryTag, multiple)
            a <- receive
          } yield a
      }
    } yield a

  def receiveContent: AKleisli[ByteVector] =
    for {
      header <- receive
      body <- receive
    } yield body

  def notifyConsumer(signal: Signal[IO, Option[Either[String, String]]], data: Either[String, String])
  : AKleisli[Unit] =
    AKleisli.eval(signal.set(Some(data)))

  def output(data: ChannelOutput): AKleisli[Unit] =
    AKleisli.applyEval(_.channel.exchange.out.enqueue1(data))

  def awaitSignal(signal: Signal[IO, Boolean]): AKleisli[Unit] =
    AKleisli.eval(signal.discrete.filter(identity).take(1).compile.drain)

  def interpreterK: ChannelA ~> AKleisli =
    new (ChannelA ~> AKleisli) {
      def apply[A](action: ChannelA[A]): AKleisli[A] =
        action match {
          case ChannelA.SendAmqpHeader =>
            send(Message.header)
          case ChannelA.SendMethod(payload) =>
            sendMethod(payload)
          case ChannelA.SendContent(classId, payload) =>
            sendContent(classId, payload)
          case ChannelA.Receive =>
            receive
          case ChannelA.ReceiveContent =>
            receiveContent
          case ChannelA.NotifyConsumer(signal, data) =>
            notifyConsumer(signal, data)
          case ChannelA.Log(message) =>
            log(message)
          case ChannelA.ConnectionOutput(comm) =>
            AKleisli.pull(Pull.output1(comm))
          case ChannelA.ChannelOpened =>
            AKleisli.pure(())
          case ChannelA.Output(data) =>
            output(data)
          case ChannelA.Eval(fa) =>
            AKleisli.eval(fa)
        }
    }

  def interpreter(connection: ChannelConnection): ChannelA ~> Effect =
    interpreterK.andThen(λ[AKleisli ~> Effect](_(connection)))
}
