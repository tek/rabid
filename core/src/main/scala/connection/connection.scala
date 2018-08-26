package rabid
package connection

import java.nio.channels.AsynchronousChannelGroup

import scala.concurrent.ExecutionContext
import scala.collection.immutable.SortedMap

import fs2.Stream
import fs2.async.mutable.Queue
import cats.~>
import cats.data.State
import cats.free.Free
import cats.effect.IO
import cats.implicits._

import channel.{ChannelConnection, Channel}

case class Connection(
  pool: Connection.ChannelPool,
  channel0: ChannelConnection,
  channels: SortedMap[Short, ChannelConnection],
  state: ConnectionState,
  buffer: Vector[Input],
)

object Connection
{
  type ChannelPool = Queue[IO, Stream[IO, Input]]

  def cons(
    pool: Connection.ChannelPool,
    channelConnection0: ChannelConnection,
  ): Connection =
    Connection(pool, channelConnection0, SortedMap.empty, ConnectionState.Disconnected, Vector.empty)

  def operation: Input => ConnectionA.Step[PNext] = {
    case Input.Connected =>
      Free.pure(PNext.Regular)
    case Input.Rabbit(message) =>
      programs.sendToRabbit(message)
    case Input.ChannelReceive(header, body) =>
      programs.sendToChannel(header, body)
    case Input.OpenChannel(request) =>
      programs.createChannel(request)
    case Input.ChannelOpened(number, id) =>
      programs.channelOpened(number, id)
  }

  def disconnected(input: Input): State[ProcessData[Input], ConnectionA.Step[PNext]] =
    for {
      _ <- Process.buffer(input)
      _ <- Process.transition(PState.Connecting)
    } yield programs.connect

  def connecting: Input => State[ProcessData[Input], ConnectionA.Step[PNext]] = {
    case Input.Connected =>
      Process.transition[Input](PState.Connected).as(programs.connected)
    case input @ Input.Rabbit(_) =>
      State.pure(operation(input))
    case input @ Input.ChannelReceive(header, _) if header.channel == 0 =>
      State.pure(operation(input))
    case a =>
      Process.bufferOnly(a)
  }

  def execute: PState => Input => State[ProcessData[Input], ConnectionA.Step[PNext]] = {
    case PState.Disconnected =>
      disconnected
    case PState.Connecting =>
      connecting
    case PState.Connected =>
      operation.andThen(State.pure)
  }

  def run(
    pool: Connection.ChannelPool,
    interpreter: ConnectionA ~> ConnectionA.Effect,
    listen: Stream[IO, Input],
  )
  (implicit ec: ExecutionContext)
  : Stream[IO, Unit] = {
    for {
      channel0 <- Stream.eval(Channel.cons)
      connection = Connection.cons(pool, ChannelConnection(0, channel0, channel0.receive))
      loop = Process.loop(interpreter, execute, ProcessData.cons("connection", PState.Disconnected), connection)
      _ <- listen.through(a => loop(a).stream)
    } yield ()
  }

  def channelOutput(channels: Connection.ChannelPool)
  (implicit ec: ExecutionContext)
  : Stream[IO, Input] =
    channels.dequeue.join(10)

  def listenChannels(pool: Connection.ChannelPool)
  (implicit ec: ExecutionContext)
  : Stream[IO, Input] =
    channelOutput(pool)

  def listen(rabbit: Stream[IO, Input], pool: Connection.ChannelPool, channels: Queue[IO, Input])
  (implicit ec: ExecutionContext)
  : Stream[IO, Input] =
    listenChannels(pool).merge(rabbit).merge(channels.dequeue)

  def using(rabbitInput: Stream[IO, Input])(interpreter: ConnectionA ~> ConnectionA.Effect)
  (implicit ec: ExecutionContext)
  : IO[(Rabid, Stream[IO, Unit])] =
    for {
      pool <- Queue.unbounded[IO, Stream[IO, Input]]
      input <- Queue.unbounded[IO, Input]
    } yield (Rabid(input), run(pool, interpreter, listen(rabbitInput, pool, input)))


  def native
  (host: String, port: Int)
  (implicit ec: ExecutionContext, ag: AsynchronousChannelGroup)
  : Stream[IO, (Rabid, Stream[IO, Unit])] =
    for {
      (rabbitInput, interpreter) <- Interpreter.native(host, port)
      (rabid, main) <- Stream.eval(using(rabbitInput)(interpreter))
    } yield (rabid, main)
}
