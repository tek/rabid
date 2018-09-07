package rabid
package connection

import java.nio.channels.AsynchronousChannelGroup
import java.nio.channels.spi.AsynchronousChannelProvider

import scala.concurrent.ExecutionContext
import scala.collection.immutable.SortedMap

import fs2.Stream
import fs2.async.mutable.Queue
import fs2.internal.ThreadFactories
import cats.~>
import cats.data.State
import cats.free.Free
import cats.effect.IO
import cats.implicits._

import channel.{ChannelConnection, Channel}

case class ConnectionResources(
  pool: Connection.ChannelPool,
  channel0: ChannelConnection,
  channels: SortedMap[Short, ChannelConnection],
  state: ConnectionState,
  buffer: Vector[Input],
)

object ConnectionResources
{
  def cons(
    pool: Connection.ChannelPool,
    channelConnection0: ChannelConnection,
  ): ConnectionResources =
    ConnectionResources(pool, channelConnection0, SortedMap.empty, ConnectionState.Disconnected, Vector.empty)
}

case class ConnectionConfig(user: String, password: String, vhost: String)

case class Connection(input: Stream[IO, Input], interpreter: Connection.Interpreter)

object Connection
{
  type ChannelPool = Queue[IO, Stream[IO, Input]]
  type Interpreter = ConnectionA ~> ConnectionA.Effect

  def operation: Input => ConnectionA.Step[PNext] = {
    case Input.Connected =>
      Free.pure(PNext.Regular)
    case Input.Rabbit(message) =>
      programs.sendToRabbit(message)
    case Input.ChannelReceive(FrameHeader(FrameType.Heartbeat, _, _), _) =>
      programs.sendToRabbit(Message.heartbeat)
    case Input.ChannelReceive(header, body) =>
      programs.sendToChannel(header, body)
    case Input.OpenChannel(request) =>
      programs.createChannel(request)
    case Input.ChannelOpened(number, id) =>
      programs.channelOpened(number, id)
  }

  def disconnected(conf: ConnectionConfig)(input: Input): State[ProcessData[Input], ConnectionA.Step[PNext]] =
    for {
      _ <- Process.buffer(input)
      _ <- Process.transition(PState.Connecting)
    } yield programs.connect(conf.user, conf.password, conf.vhost)

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

  def execute(conf: ConnectionConfig): PState => Input => State[ProcessData[Input], ConnectionA.Step[PNext]] = {
    case PState.Disconnected =>
      disconnected(conf)
    case PState.Connecting =>
      connecting
    case PState.Connected =>
      operation.andThen(State.pure)
  }

  def run(
    pool: Connection.ChannelPool,
    interpreter: Interpreter,
    listen: Stream[IO, Input],
    conf: ConnectionConfig,
  )
  (implicit ec: ExecutionContext)
  : Stream[IO, Unit] = {
    for {
      channel0 <- Stream.eval(Channel.cons)
      connection = ConnectionResources.cons(pool, ChannelConnection(0, channel0, channel0.receive))
      loop = Process.loop(interpreter, execute(conf), ProcessData.cons("connection", PState.Disconnected), connection)
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

  def start(connection: Connection, conf: ConnectionConfig)
  (implicit ec: ExecutionContext)
  : IO[(Rabid, Stream[IO, Unit])] =
    for {
      pool <- Queue.unbounded[IO, Stream[IO, Input]]
      consumerInput <- Queue.unbounded[IO, Input]
    } yield {
      (Rabid(consumerInput), run(pool, connection.interpreter, listen(connection.input, pool, consumerInput), conf))
    }

  implicit def tcpACG: AsynchronousChannelGroup =
    AsynchronousChannelProvider
      .provider()
      .openAsynchronousChannelGroup(1, ThreadFactories.named("rabbit", true))

  def native
  (host: String, port: Int)
  (implicit ec: ExecutionContext)
  : Stream[IO, Connection] =
    for {
      (input, interpreter) <- Interpreter.native(host, port)
    } yield Connection(input, interpreter)
}
