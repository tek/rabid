package rabid
package connection

import cats.free.Free
import cats.implicits._

import channel.{Channel, ChannelProg}

object programs
{
  def sendToRabbit(message: Message): Action.Step[Continuation] =
    Action.liftF(Action.Send(message)).as(Continuation.Regular)

  def sendToChannel(header: FrameHeader, body: FrameBody): Action.Step[Continuation] =
    Action.liftF(Action.SendToChannel(header, body)).as(Continuation.Regular)

  def createChannel(channel: Channel): Action.Step[Continuation] =
    Action.liftF(Action.CreateChannel(channel)).as(Continuation.Regular)

  def channelCreated(number: Short, id: String): Action.Step[Continuation] =
    Action.liftF(Action.ChannelCreated(number, id)).as(Continuation.Regular)

  def exit: Action.Step[Continuation] =
    Free.pure(Continuation.Exit)

  def connected: Action.Step[Continuation] =
    for {
      _ <- Action.liftF(Action.SetConnected(true))
      _ <- Action.liftF(Action.RunInControlChannel(
        ChannelProg("listen in control channel", channel.programs.controlListen)))
    } yield Continuation.Debuffer

  def connect: Action.Step[Continuation] =
    for {
      _ <- Action.liftF(Action.StartControlChannel)
      _ <- Action.liftF(Action.RunInControlChannel(ChannelProg("connect to server", channel.programs.connect)))
    } yield Continuation.Regular
}
