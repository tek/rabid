package rabid

import cats.effect.Sync
import cats.implicits._
import fs2.Pull
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

object Log
{
  object pull
  {
    def apply[F[_]: Sync](handler: (String, String) => F[Unit])(name: String, message: String)
    : Pull[F, Nothing, Unit] =
      Pull.eval(handler(name, message))

    def error[F[_]: Sync](name: String, message: String): Pull[F, Nothing, Unit] =
      apply(Log.error[F])(name, message)

    def info[F[_]: Sync](name: String, message: String): Pull[F, Nothing, Unit] =
      apply(Log.info[F])(name, message)
  }

  def apply[F[_]: Sync](handler: Logger[F] => String => F[Unit])(name: String, message: String): F[Unit] =
    for {
      logger <- Slf4jLogger.fromName[F](name)
      _ <- handler(logger)(message)
    } yield ()

  def info[F[_]: Sync](name: String, message: String): F[Unit] =
    apply[F](a => a.info(_))(name, message)

  def error[F[_]: Sync](name: String, message: String): F[Unit] =
    apply[F](a => a.error(_))(name, message)
}
