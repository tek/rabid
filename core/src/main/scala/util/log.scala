package rabid

import cats.effect.IO
import fs2.Pull
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

object Log
{
  object pull
  {
    def apply(handler: (String, String) => IO[Unit])(name: String, message: String): Pull[IO, Nothing, Unit] =
      Pull.eval(handler(name, message))

    def error: (String, String) => Pull[IO, Nothing, Unit] =
      apply(Log.error)

    def info: (String, String) => Pull[IO, Nothing, Unit] =
      apply(Log.info)
  }

  def apply(handler: Logger[IO] => String => IO[Unit])(name: String, message: String): IO[Unit] =
    for {
      logger <- Slf4jLogger.fromName[IO](name)
      _ <- handler(logger)(message)
    } yield ()

  def info: (String, String) => IO[Unit] =
    apply(a => a.info(_))

  def error: (String, String) => IO[Unit] =
    apply(a => a.error(_))
}
