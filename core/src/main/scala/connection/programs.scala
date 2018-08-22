package rabid
package connection

import cats.free.Free
import cats.implicits._

import channel.{Channel, ChannelInput}

object programs
{
  def sendToRabbit(message: Message): Action.Step[PNext] =
    Action.liftF(Action.Send(message)).as(PNext.Regular)

  def sendToChannel(header: FrameHeader, body: FrameBody): Action.Step[PNext] =
    Action.liftF(Action.ChannelReceive(header, body)).as(PNext.Regular)

  def createChannel(channel: Channel): Action.Step[PNext] =
    Action.liftF(Action.OpenChannel(channel)).as(PNext.Regular)

  def channelOpened(number: Short, id: String): Action.Step[PNext] =
    for {
      _ <- Action.liftF(Action.NotifyChannel(number, ChannelInput.Opened))
      _ <- Action.liftF(Action.ChannelOpened(number, id))
    } yield PNext.Regular

  def exit: Action.Step[PNext] =
    Free.pure(PNext.Exit)

  def connected: Action.Step[PNext] =
    for {
      _ <- Action.liftF(Action.RunInControlChannel(
        ChannelInput.Prog("listen in control channel", channel.programs.controlListen)))
    } yield PNext.Debuffer

  def connect: Action.Step[PNext] =
    for {
      _ <- Action.liftF(Action.StartControlChannel)
      _ <- Action.liftF(Action.RunInControlChannel(ChannelInput.Prog("connect to server", channel.programs.connect)))
    } yield PNext.Regular
}
