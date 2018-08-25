package rabid
package connection

import cats.free.Free
import cats.implicits._

import channel.{Channel, ChannelInput}

object programs
{
  def sendToRabbit(message: Message): ConnectionA.Step[PNext] =
    ConnectionA.liftF(ConnectionA.Send(message)).as(PNext.Regular)

  def sendToChannel(header: FrameHeader, body: FrameBody): ConnectionA.Step[PNext] =
    ConnectionA.liftF(ConnectionA.ChannelReceive(header, body)).as(PNext.Regular)

  def createChannel(channel: Channel): ConnectionA.Step[PNext] =
    ConnectionA.liftF(ConnectionA.OpenChannel(channel)).as(PNext.Regular)

  def channelOpened(number: Short, id: String): ConnectionA.Step[PNext] =
    for {
      _ <- ConnectionA.liftF(ConnectionA.NotifyChannel(number, ChannelInput.Opened))
      _ <- ConnectionA.liftF(ConnectionA.ChannelOpened(number, id))
    } yield PNext.Regular

  def exit: ConnectionA.Step[PNext] =
    Free.pure(PNext.Exit)

  def connected: ConnectionA.Step[PNext] =
    for {
      _ <- ConnectionA.liftF(ConnectionA.RunInControlChannel(
        ChannelInput.Internal("listen in control channel", channel.programs.controlListen)))
    } yield PNext.Debuffer

  def connect: ConnectionA.Step[PNext] =
    for {
      _ <- ConnectionA.liftF(ConnectionA.StartControlChannel)
      _ <- ConnectionA.liftF(ConnectionA.RunInControlChannel(ChannelInput.Internal("connect to server", channel.programs.connect)))
    } yield PNext.Regular
}
