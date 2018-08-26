package rabid
package channel

import scala.concurrent.ExecutionContext

import fs2.Stream
import fs2.async.mutable.Queue
import cats.implicits._
import cats.data.State
import cats.free.Free
import cats.effect.IO
import scodec.bits.ByteVector

import connection.{Exchange, Input}

case class Channel(
  exchange: Exchange[ChannelInput, ChannelOutput],
  receive: Queue[IO, Either[ChannelInterrupt, ByteVector]],
)

object Channel
{
  def cons(implicit ec: ExecutionContext): IO[Channel] =
    for {
      in <- Queue.unbounded[IO, ChannelInput]
      out <- Queue.unbounded[IO, ChannelOutput]
      receive <- Queue.unbounded[IO, Either[ChannelInterrupt, ByteVector]]
    } yield Channel(Exchange(in, out), receive)

  type Prog = ChannelA.Step[PNext]

  def disconnected(number: Short)(input: ChannelInput): State[ProcessData[ChannelInput], ChannelA.Internal] =
    for {
      _ <- Process.buffer(input)
      _ <- Process.transition(PState.Connecting)
    } yield programs.createChannel(number)

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

  def execute(number: Short): PState => ChannelInput => State[ProcessData[ChannelInput], ChannelA.Internal] = {
    case PState.Disconnected =>
      disconnected(number)
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
    val loop = Process.loop(Interpreter.interpreter(channel), execute(channel.number), processData, data)
    channelInput(channel).through(a => loop(a).stream)
  }

  def runControl(channel: ChannelConnection)
  : Stream[IO, Input] =
    runChannel(channel, PState.Connected)

  def run(channel: ChannelConnection)
  : Stream[IO, Input] =
    runChannel(channel, PState.Disconnected)
}
