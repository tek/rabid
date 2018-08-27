package rabid

import java.util.concurrent._
import scala.concurrent.ExecutionContext

object EC
{
  def threadFactory: ThreadFactory =
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

  val cpus: Int = Runtime.getRuntime.availableProcessors

  def pool(max: Int): ThreadPoolExecutor =
    new ThreadPoolExecutor(
      cpus,
      math.max(max, (cpus.toDouble * 3f).ceil.toInt),
      10L,
      TimeUnit.SECONDS,
      new LinkedBlockingQueue[Runnable],
      threadFactory,
    )

  def apply(max: Int = 10): ExecutionContext =
    ExecutionContext.fromExecutorService(pool(max))
}
