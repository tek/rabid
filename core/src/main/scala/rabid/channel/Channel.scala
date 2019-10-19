package rabid
package channel

import cats.data.State
import cats.effect.{ContextShift, IO}
import cats.free.Free
import cats.implicits._
import connection.{Exchange, Input}
import fs2.Stream
import fs2.concurrent.Queue

case class Channel(
  exchange: Exchange[ChannelInput, ChannelOutput],
  receive: Queue[IO, ChannelMessage],
)

object Channel
{
  def cons(implicit cs: ContextShift[IO]): IO[Channel] =
    for {
      in <- Queue.unbounded[IO, ChannelInput]
      out <- Queue.unbounded[IO, ChannelOutput]
      receive <- Queue.unbounded[IO, ChannelMessage]
    } yield Channel(Exchange(in, out), receive)

  type Prog = ChannelA.Step[PNext]

  def disconnected(number: Short, conf: QosConf)(input: ChannelInput)
  : State[ProcessData[ChannelInput], ChannelA.Internal] =
    for {
      _ <- Process.buffer(input)
      _ <- Process.transition(PState.Connecting)
    } yield programs.createChannel(number, conf)

  def connecting: ChannelInput => State[ProcessData[ChannelInput], ChannelA.Internal] = {
    case ChannelInput.Opened =>
      Process.transition[ChannelInput](PState.Connected).as(Free.pure(PNext.Debuffer))
    case input => Process.bufferOnly(input)
  }

  def operation: ChannelInput => ChannelA.Internal = {
    case ChannelInput.Internal(name, thunk) =>
      Actions.log(s"running internal channel prog `$name`") >> thunk
    case ChannelInput.Prog(name, thunk) =>
      Actions.log(s"running channel prog `$name`") >> thunk
    case ChannelInput.Sync(name, thunk) =>
      Actions.log(s"running sync channel prog `$name`") >> programs.syncProg(thunk)
    case ChannelInput.Opened => ChannelA.pure(PNext.Debuffer)
    case ChannelInput.StopConsumer =>
      ChannelA.pure(PNext.Exit)
  }

  def execute(number: Short, conf: QosConf)
  : PState => ChannelInput => State[ProcessData[ChannelInput], ChannelA.Internal] = {
    case PState.Disconnected =>
      disconnected(number, conf)
    case PState.Connecting =>
      connecting
    case PState.Connected =>
      operation.andThen(State.pure)
  }

  def channelInput(connection: ChannelConnection)
  : Stream[IO, ChannelInput] =
    connection.channel.exchange.in.dequeue

  def runChannel(channel: ChannelConnection, state: PState)
  : Stream[IO, Input] = {
    val processData = ProcessData.cons[ChannelInput]("connection", state)
    val data = ChannelData.cons(channel.number, state)
    val loop = Process.loop(Interpreter.interpreter(channel), execute(channel.number, channel.qos), processData, data)
    channelInput(channel).through(a => loop(a).stream)
  }

  def runControl(channel: ChannelConnection)
  : Stream[IO, Input] =
    runChannel(channel, PState.Connected)

  def run(channel: ChannelConnection)
  : Stream[IO, Input] =
    runChannel(channel, PState.Disconnected)
}
