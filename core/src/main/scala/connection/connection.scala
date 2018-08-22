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

  def operation: Input => Action.Step[PNext] = {
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

  def disconnected(input: Input): State[ProcessData[Input], Action.Step[PNext]] =
    for {
      _ <- Process.buffer(input)
      _ <- Process.transition(PState.Connecting)
    } yield programs.connect

  def connecting: Input => State[ProcessData[Input], Action.Step[PNext]] = {
    case Input.Connected =>
      Process.transition[Input](PState.Connected).as(programs.connected)
    case input @ Input.Rabbit(_) =>
      State.pure(operation(input))
    case input @ Input.ChannelReceive(header, _) if header.channel == 0 =>
      State.pure(operation(input))
    case a =>
      Process.bufferOnly(a)
  }

  def execute: PState => Input => State[ProcessData[Input], Action.Step[PNext]] = {
    case PState.Disconnected =>
      disconnected
    case PState.Connecting =>
      connecting
    case PState.Connected =>
      operation.andThen(State.pure)
  }

  def run(
    pool: Connection.ChannelPool,
    interpreter: Action ~> Action.Effect,
    listen: Stream[IO, Input],
  )
  (implicit ec: ExecutionContext)
  : Stream[IO, Unit] = {
    for {
      channel0 <- Stream.eval(Channel.cons)
      channelConnection0 <- Stream.eval(ChannelConnection.cons(0, channel0))
      connection = Connection.cons(pool, channelConnection0)
      loop = Process.loop(interpreter, execute, ProcessData.cons("connection", PState.Disconnected), connection)
      _ <- listen.through(a => loop(a).stream)
    } yield ()
  }

  def native
  (host: String, port: Int)
  (implicit ec: ExecutionContext, ag: AsynchronousChannelGroup)
  : Stream[IO, (Rabid, Stream[IO, Unit], IO[Unit])] =
    for {
      (pool, listen, input, interpreter, close) <- Interpreter.native(host, port)
    } yield (Rabid(input), run(pool, interpreter, listen), close)
}
