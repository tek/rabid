package rabid

import cats.{Applicative, FlatMap, Functor, ~>}
import cats.data.{EitherT, State, StateT}
import cats.effect.Sync
import cats.free.Free
import cats.implicits._
import fs2.{Pull, Stream}
import scodec.{Attempt, Err}

sealed trait PState

object PState
{
  case object Disconnected
  extends PState

  case object Connecting
  extends PState

  case object Connected
  extends PState
}

sealed trait PNext

object PNext
{
  case object Regular
  extends PNext

  case object Debuffer
  extends PNext

  case object Exit
  extends PNext
}

case class ProcessData[A](logger: String, state: PState, buffer: Vector[A])

object ProcessData
{
  def cons[A](logger: String, state: PState): ProcessData[A] = ProcessData(logger, state, Vector.empty)
}

object Process
{
  type Step[Alg[_], A] = Free[λ[a => Attempt[Alg[a]]], A]

  type ST[F[_], I, A] = StateT[F, ProcessData[I], A]

  object ST
  {
    def pure[F[_]: Applicative, I, A](a: A): ST[F, I, A] =
      StateT.pure[F, ProcessData[I], A](a)

    def liftF[F[_]: Applicative, I, A](fa: F[A]): ST[F, I, A] =
      StateT.liftF(fa)

    def inspect[F[_]: Applicative, I, A](f: ProcessData[I] => A): StateT[F, ProcessData[I], A] =
      StateT.inspect[F, ProcessData[I], A](f)
  }

  type EST[F[_], I, O, D, A] = StateT[Pull[F, O, ?], D, A]

  type Effect[F[_], I, O, D, A] = EitherT[EST[F, I, O, D, ?], Err, A]

  def buffer[I](a: I): State[ProcessData[I], Unit] =
    State.modify(s => s.copy(buffer = s.buffer :+ a))

  def bufferOnly[A[_], I](a: I): State[ProcessData[I], Step[A, PNext]] =
    buffer(a).as(Free.pure[λ[a => Attempt[A[a]]], PNext](PNext.Regular))

  def transition[I](state: PState): State[ProcessData[I], Unit] =
    State.modify(s => s.copy(state = state))

  def debuffer[F[_]: Sync, I]
  : StateT[F, ProcessData[I], Vector[I]] =
    for {
      buffered <- ST.inspect[F, I, Vector[I]](_.buffer)
      _ <- {
        if (buffered.isEmpty) ST.pure[F, I, Unit](())
        else
          for {
            _ <- StateT.modify[F, ProcessData[I]](_.copy(buffer = Vector.empty))
            logger <- ST.inspect[F, I, String](_.logger)
            _ <- ST.liftF(Log.info(logger, s"rebuffering inputs $buffered"))
          } yield ()
      }
    } yield buffered

  def continuation[F[_]: Sync, I](tail: Stream[F, I])
  : Either[Err, PNext] => StateT[F, ProcessData[I], Stream[F, I]] = {
    case Right(PNext.Regular) =>
      StateT.pure(tail)
    case Right(PNext.Debuffer) =>
      for {
        debuffered <- debuffer[F, I]
      } yield Stream.emits(debuffered) ++ tail
    case Right(PNext.Exit) =>
      StateT.pure(Stream.empty)
    case Left(err) =>
      for {
        logger <- StateT.inspect[F, ProcessData[I], String](_.logger)
        _ <- StateT.liftF[F, ProcessData[I], Unit](Log.error(logger, s"error in channel program: $err"))
      } yield tail
  }

  def interpretAttempt[A[_], F[_], D, I, O](inner: A ~> Effect[F, I, O, D, ?])
  : λ[a => Attempt[A[a]]] ~> Effect[F, I, O, D, ?] =
    new (λ[a => Attempt[A[a]]] ~> Effect[F, I, O, D, ?]) {
      def apply[B](a: Attempt[A[B]]): Effect[F, I, O, D, B] = {
        a match {
          case Attempt.Successful(a) => inner(a)
          case Attempt.Failure(err) => EitherT.leftT[EST[F, I, O, D, ?], B](err)
        }
      }
    }

  def zoomRight[F[_]: Functor, S, R, A](st: StateT[F, R, A]): StateT[F, (S, R), A] =
    st.transformS[(S, R)](_._2, (a, b) => (a._1, b))

  def zoomLeft[F[_]: Functor, S, R, A](st: StateT[F, S, A]): StateT[F, (S, R), A] =
    st.transformS[(S, R)](_._1, (a, b) => (b, a._2))

  def liftPull[F[_]: FlatMap, S, A, O](st: StateT[F, S, A]): StateT[Pull[F, O, ?], S, A] =
    st.transformF(Pull.eval)

  def interpret[F[_]: Sync, I, O, A[_], D](
    interpreter: A ~> Effect[F, I, O, D, ?],
    program: Step[A, PNext],
    tail: Stream[F, I]
  ): EST[F, I, O, (ProcessData[I], D), Stream[F, I]] =
    for {
      output <- zoomRight(program.foldMap(interpretAttempt(interpreter)).value)
      cont <- zoomLeft(liftPull[F, ProcessData[I], Stream[F, I], O](continuation(tail).apply(output)))
    } yield cont

  def process[F[_]: Sync, I, O, A[_], D](
    interpreter: A ~> Effect[F, I, O, D, ?],
    execute: PState => I => State[ProcessData[I], Step[A, PNext]],
    processData: ProcessData[I],
    data: D,
  ): Option[(I, Stream[F, I])] => Pull[F, O, Unit] = {
    case Some((a, tail)) =>
      val (data1, program) = execute(processData.state)(a).run(processData).value
      for {
        _ <- Log.pull.info[F](processData.logger, s"received input $a")
        ((processData1, data2), next) <- interpret(interpreter, program, tail).run((data1, data))
        _ <- loop(interpreter, execute, processData1, data2).apply(next)
      } yield ()
    case None => Pull.done
  }

  def loop[F[_]: Sync, I, O, A[_], D](
    interpreter: A ~> Effect[F, I, O, D, ?],
    execute: PState => I => State[ProcessData[I], Step[A, PNext]],
    processData: ProcessData[I],
    data: D,
  ): Stream[F, I] => Pull[F, O, Unit] =
    inputs =>
      for {
        input <- inputs.pull.uncons1
        _ <- process(interpreter, execute, processData, data).apply(input)
      } yield ()
}
