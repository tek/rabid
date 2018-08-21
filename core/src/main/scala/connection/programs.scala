package rabid
package connection

import cats.free.Free
import cats.implicits._

import channel.{Channel, ChannelInput}

object programs
{
  def sendToRabbit(message: Message): Action.Step[Continuation] =
    Action.liftF(Action.Send(message)).as(Continuation.Regular)

  def sendToChannel(header: FrameHeader, body: FrameBody): Action.Step[Continuation] =
    Action.liftF(Action.ChannelReceive(header, body)).as(Continuation.Regular)

  def createChannel(channel: Channel): Action.Step[Continuation] =
    Action.liftF(Action.OpenChannel(channel)).as(Continuation.Regular)

  def channelOpened(number: Short, id: String): Action.Step[Continuation] =
    for {
      _ <- Action.liftF(Action.NotifyChannel(number, ChannelInput.Opened))
      _ <- Action.liftF(Action.ChannelOpened(number, id))
    } yield Continuation.Regular

  def exit: Action.Step[Continuation] =
    Free.pure(Continuation.Exit)

  def connected: Action.Step[Continuation] =
    for {
      _ <- Action.liftF(Action.RunInControlChannel(
        ChannelInput.Prog("listen in control channel", channel.programs.controlListen)))
    } yield Continuation.Debuffer

  def connect: Action.Step[Continuation] =
    for {
      _ <- Action.liftF(Action.StartControlChannel)
      _ <- Action.liftF(Action.RunInControlChannel(ChannelInput.Prog("connect to server", channel.programs.connect)))
    } yield Continuation.Regular
}
