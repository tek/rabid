package rabid

import java.util.concurrent._

import scala.concurrent.ExecutionContext

import cats.effect.{ContextShift, IO, Resource}
import fs2.Stream

object Concurrency
{
  def threadFactory: IO[ThreadFactory] =
    IO {
      new ThreadFactory {
        val count = new atomic.AtomicLong(0)
        val factory = Executors.defaultThreadFactory
        override def newThread(r: Runnable): Thread = {
          val thread = factory.newThread(r)
          thread.setName(s"rabid-${count.getAndIncrement}")
          thread.setDaemon(true)
          thread
        }
      }
    }

  val defaultNum: Int =
    Runtime.getRuntime.availableProcessors

  def pool(max: Int): Resource[IO, ThreadPoolExecutor] =
    Resource.make(
      threadFactory.map(fact =>
        new ThreadPoolExecutor(
          defaultNum,
          math.max(max, (defaultNum.toDouble * 3f).ceil.toInt),
          10L,
          TimeUnit.SECONDS,
          new LinkedBlockingQueue[Runnable],
          fact,
        )
      )
    )(es => IO(es.shutdown()))

  def ec(pool: IO[ExecutorService]): Resource[IO, ExecutionContext] =
    Resource.make(pool)(es => IO(es.shutdown()))
      .map(ExecutionContext.fromExecutorService)

  def fixedPoolWith(num: Int): IO[ExecutorService] =
    IO(Executors.newFixedThreadPool(num))

  def fixedPool: IO[ExecutorService] =
    fixedPoolWith(defaultNum)

  def fixedPoolEc: Resource[IO, ExecutionContext] =
    ec(fixedPool)

  def fixedPoolEcWith(num: Int): Resource[IO, ExecutionContext] =
    ec(fixedPoolWith(num))

  def fixedPoolEcStreamWith(num: Int): Stream[IO, ExecutionContext] =
    Stream.resource(fixedPoolEcWith(num))

  def fixedPoolEcStream: Stream[IO, ExecutionContext] =
    Stream.resource(fixedPoolEc)

  def cs(pool: IO[ExecutorService]): Resource[IO, ContextShift[IO]] =
    ec(pool)
      .map(IO.contextShift(_))

  def fixedPoolCsWith(num: Int): Resource[IO, ContextShift[IO]] =
    cs(fixedPoolWith(num))

  def fixedPoolCs: Resource[IO, ContextShift[IO]] =
    cs(fixedPool)

  def fixedPoolCsStreamWith(num: Int): Stream[IO, ContextShift[IO]] =
    Stream.resource(fixedPoolCsWith(num))

  def fixedPoolCsStream: Stream[IO, ContextShift[IO]] =
    Stream.resource(fixedPoolCs)
}
