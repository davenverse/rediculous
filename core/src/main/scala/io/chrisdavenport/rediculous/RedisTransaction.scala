package io.chrisdavenport.rediculous

import cats._
import cats.implicits._
import cats.data._
import cats.effect._
import RedisProtocol._


object RedisTransaction {

  final case class TxQueued[A](value: RedisTx[Queued[A]])
  object TxQueued {
    implicit val ctx: RedisCtx[TxQueued] =  new RedisCtx[TxQueued]{
      def run[A: RedisResult](command: NonEmptyList[String]): TxQueued[A] = TxQueued(RedisTx{for {
        (i, base) <- State.get
        _ <- State.set((i + 1, base ++ List(command)))
      } yield Queued(l => RedisResult[A].decode(l(i)))})
    }
    implicit val applicative: Applicative[TxQueued] = new Applicative[TxQueued]{
      def pure[A](a: A) = TxQueued(Monad[RedisTx].pure(Monad[Queued].pure(a)))

      override def ap[A, B](ff: TxQueued[A => B])(fa: TxQueued[A]): TxQueued[B] =
        TxQueued(RedisTx(
          Nested(ff.value.value).ap(Nested(fa.value.value)).value
        ))
    }
  }

  final case class RedisTx[A](value: State[(Int, List[NonEmptyList[String]]), A])
  object RedisTx {

    implicit val m: Monad[RedisTx] = new StackSafeMonad[RedisTx]{
      def pure[A](a: A): RedisTx[A] = RedisTx(Monad[State[(Int, List[NonEmptyList[String]]), *]].pure(a))
      def flatMap[A, B](fa: RedisTx[A])(f: A => RedisTx[B]): RedisTx[B] = RedisTx(
        fa.value.flatMap(f.andThen(_.value))
      )
    }
  }
  final case class Queued[A](f: List[Resp] => Either[Resp, A])
  object Queued {
    implicit val m: Monad[Queued] = new StackSafeMonad[Queued]{
      def pure[A](a: A) = Queued{_ => Either.right(a)}
      def flatMap[A, B](fa: Queued[A])(f: A => Queued[B]): Queued[B] = {
        Queued{l => 
          for {
            a <- fa.f(l)
            b  <- f(a).f(l)
          } yield b
        }
      }
    }
  }

  sealed trait TxResult[+A]
  object TxResult {
    final case class Success[A](value: A) extends TxResult[A]
    final case object Aborted extends TxResult[Nothing]
    final case class Error(value: String) extends TxResult[Nothing]
  }

  def watch[F[_]: Concurrent](keys: List[String]): Redis[F, Status] = 
    RedisCtx[Redis[F,*]].run(NonEmptyList("WATCH", keys))

  def unwatch[F[_]: Concurrent]: Redis[F, Status] = 
    RedisCtx[Redis[F,*]].run(NonEmptyList.of("UNWATCH"))

  def multiExec[F[_]] = new MultiExecPartiallyApplied[F]
  
  class MultiExecPartiallyApplied[F[_]]{

    def apply[A](tx: TxQueued[A])(implicit F: Concurrent[F]): Redis[F, TxResult[A]] = {
      Redis(Kleisli{c: RedisConnection[F] => 
        val ((_, commands), Queued(f)) = tx.value.value.run((0, List.empty)).value
        RedisConnection.runRequestInternal(c)(NonEmptyList(
          NonEmptyList.of("MULTI"),
          commands ++ 
          List(NonEmptyList.of("EXEC"))
        )).map{_.flatMap{_.last match {
          case Resp.Array(Some(a)) => f(a).fold[TxResult[A]](e => TxResult.Error(e.toString), TxResult.Success(_)).pure[F]
          case Resp.Array(None) => (TxResult.Aborted: TxResult[A]).pure[F]
          case other => ApplicativeError[F, Throwable].raiseError(new Throwable(s"EXEC returned $other"))
        }}}
      })
    }
  }



}