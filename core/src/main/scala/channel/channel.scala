package rabid
package channel

import scala.concurrent.ExecutionContext

import fs2.Stream
import fs2.async.mutable.Queue
import scodec.bits.ByteVector
import cats.implicits._
import cats.data.State
import cats.free.Free
import cats.effect.IO

import connection.{Exchange, ConsumerResponse, Input}

case class ChannelOutput(number: Short)

sealed trait ChannelInput

object ChannelInput
{
  case class Prog(name: String, thunk: Channel.Prog)
  extends ChannelInput

  case object Opened
  extends ChannelInput
}

case class ChannelConnection(
  number: Short,
  progs: Queue[IO, ChannelInput],
  consumerProgs: Queue[IO, ChannelInput],
  receive: Queue[IO, ByteVector],
)

object ChannelConnection
{
  def cons(number: Short, channel: Channel)
  (implicit ec: ExecutionContext)
  : IO[ChannelConnection] = {
    for {
      in <- Queue.unbounded[IO, ChannelInput]
      input <- Queue.unbounded[IO, ByteVector]
    } yield ChannelConnection(number, in, channel.exchange.in, input)
  }
}

case class Channel(exchange: Exchange[ChannelInput, ConsumerResponse])

case class ChannelData(number: Short, state: PState, buffer: Vector[ChannelInput])

object ChannelData
{
  def cons(number: Short, state: PState): ChannelData =
    ChannelData(number, state, Vector.empty)
}

object Channel
{
  def cons(implicit ec: ExecutionContext): IO[Channel] =
    for {
      in <- Queue.unbounded[IO, ChannelInput]
      out <- Queue.unbounded[IO, ConsumerResponse]
    } yield Channel(Exchange(in, out))

  type Prog = ChannelA.Step[PNext]

  def disconnected(number: Short)(input: ChannelInput): State[ProcessData[ChannelInput], ChannelA.Step[PNext]] =
    for {
      _ <- Process.buffer(input)
      _ <- Process.transition(PState.Connecting)
    } yield programs.createChannel(number)

  def connecting: ChannelInput => State[ProcessData[ChannelInput], ChannelA.Step[PNext]] = {
    case ChannelInput.Opened =>
      Process.transition[ChannelInput](PState.Connected).as(Free.pure(PNext.Debuffer))
    case input => Process.bufferOnly(input)
  }

  def operation: ChannelInput => ChannelA.Step[PNext] = {
    case ChannelInput.Prog(name, thunk) =>
      Actions.log(s"running channel prog `$name`") >> thunk
    case _ => ChannelA.pure(PNext.Debuffer)
  }

  def execute(number: Short): PState => ChannelInput => State[ProcessData[ChannelInput], ChannelA.Step[PNext]] = {
    case PState.Disconnected =>
      disconnected(number)
    case PState.Connecting =>
      connecting
    case PState.Connected =>
      operation.andThen(State.pure)
  }

  def channelInput(channel: ChannelConnection)
  (implicit ec: ExecutionContext)
  : Stream[IO, ChannelInput] =
    channel.progs.dequeue.merge(channel.consumerProgs.dequeue)

  def runChannel(channel: ChannelConnection, state: PState)
  (implicit ec: ExecutionContext)
  : Stream[IO, Input] = {
    val processData = ProcessData.cons[ChannelInput]("connection", state)
    val data = ChannelData.cons(channel.number, state)
    val loop = Process.loop(Interpreter.interpreter(channel), execute(channel.number), processData, data)
    channelInput(channel).through(a => loop(a).stream)
  }

  def runControl(channel: ChannelConnection)
  (implicit ec: ExecutionContext)
  : Stream[IO, Input] =
    runChannel(channel, PState.Connected)

  def run(channel: ChannelConnection)
  (implicit ec: ExecutionContext)
  : Stream[IO, Input] =
    runChannel(channel, PState.Disconnected)
}
