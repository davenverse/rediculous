package io.chrisdavenport.rediculous

import cats._
import cats.implicits._
import cats.data._
import cats.effect._
import RedisProtocol._


/**
 * Transactions Operate via typeclasses. RedisCtx allows us to abstract our operations into
 * different types depending on the behavior we want. In the case of transactions that is 
 * [[RedisTransaction]]. These can be composed together via its Applicative
 * instance to form a transaction consisting of multiple commands, then transacted via
 * either multiExec or transact on the class.
 * 
 * @example 
 * {{{
 * import io.chrisdavenport.rediculous._
 * import cats.effect.Concurrent
 * val tx = (
 *   RedisCommands.ping[RedisTransaction],
 *   RedisCommands.del[RedisTransaction](List("foo")),
 *   RedisCommands.get[RedisTransaction]("foo"),
 *   RedisCommands.set[RedisTransaction]("foo", "value"),
 *   RedisCommands.get[RedisTransaction]("foo")
 * ).tupled
 * 
 * def operation[F[_]: Concurrent] = tx.transact[F]
 * }}}
 **/
final case class RedisTransaction[A](value: RedisTransaction.RedisTxState[RedisTransaction.Queued[A]]){
  def transact[F[_]: Concurrent]: Redis[F, RedisTransaction.TxResult[A]] = 
    RedisTransaction.multiExec[F](this)
}

object RedisTransaction {

  implicit val ctx: RedisCtx[RedisTransaction] =  new RedisCtx[RedisTransaction]{
    def run[A: RedisResult](command: NonEmptyList[String]): RedisTransaction[A] = RedisTransaction(RedisTxState{for {
      (i, base) <- State.get
      _ <- State.set((i + 1, command :: base))
    } yield Queued(l => RedisResult[A].decode(l(i)))})
  }
  implicit val applicative: Applicative[RedisTransaction] = new Applicative[RedisTransaction]{
    def pure[A](a: A) = RedisTransaction(Monad[RedisTxState].pure(Monad[Queued].pure(a)))

    override def ap[A, B](ff: RedisTransaction[A => B])(fa: RedisTransaction[A]): RedisTransaction[B] =
      RedisTransaction(RedisTxState(
        Nested(ff.value.value).ap(Nested(fa.value.value)).value
      ))
  }

  /**
    * A TxResult Represent the state of a RedisTransaction when run.
    * Success means it completed succesfully, Aborted means we received
    * a Nil Arrary from Redis which represent that at least one key being watched
    * has been modified. An error occurs depending on the succesful execution of
    * the function built in Queued.
    */
  sealed trait TxResult[+A]
  object TxResult {
    final case class Success[A](value: A) extends TxResult[A]
    final case object Aborted extends TxResult[Nothing]
    final case class Error(value: String) extends TxResult[Nothing]
  }

  final case class RedisTxState[A](value: State[(Int, List[NonEmptyList[String]]), A])
  object RedisTxState {

    implicit val m: Monad[RedisTxState] = new StackSafeMonad[RedisTxState]{
      def pure[A](a: A): RedisTxState[A] = RedisTxState(Monad[State[(Int, List[NonEmptyList[String]]), *]].pure(a))
      def flatMap[A, B](fa: RedisTxState[A])(f: A => RedisTxState[B]): RedisTxState[B] = RedisTxState(
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

  // ----------
  // Operations
  // ----------
  def watch[F[_]: Concurrent](keys: List[String]): Redis[F, Status] = 
    RedisCtx[Redis[F,*]].run(NonEmptyList("WATCH", keys))

  def unwatch[F[_]: Concurrent]: Redis[F, Status] = 
    RedisCtx[Redis[F,*]].run(NonEmptyList.of("UNWATCH"))

  def multiExec[F[_]] = new MultiExecPartiallyApplied[F]
  
  class MultiExecPartiallyApplied[F[_]]{

    def apply[A](tx: RedisTransaction[A])(implicit F: Concurrent[F]): Redis[F, TxResult[A]] = {
      Redis(Kleisli{c: RedisConnection[F] => 
        val ((_, commandsR), Queued(f)) = tx.value.value.run((0, List.empty)).value
        val commands = commandsR.reverse
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