package io.chrisdavenport.rediculous

import cats.data._
import cats.implicits._
import cats._
import cats.effect._

final case class Redis[F[_], A](unRedis: Kleisli[F, RedisConnection[F], F[A]]){
  def run(connection: RedisConnection[F])(implicit ev: Monad[F]): F[A] = {
    Redis.runRedis(this)(connection)
  }
}
object Redis {

  private def runRedis[F[_]: Monad, A](redis: Redis[F, A])(connection: RedisConnection[F]): F[A] = {
    redis.unRedis.run(connection).flatten
  }

  def liftF[F[_]: Monad, A](fa: F[A]): Redis[F, A] = 
    Redis(Kleisli.liftF[F, RedisConnection[F], A](fa).map(_.pure[F]))

  /**
   * Newtype encoding for a `Redis` datatype that has a `cats.Applicative`
   * capable of doing parallel processing in `ap` and `map2`, needed
   * for implementing `cats.Parallel`.
   *
   * Helpers are provided for converting back and forth in `Par.apply`
   * for wrapping any `Redis` value and `Par.unwrap` for unwrapping.
   *
   * The encoding is based on the "newtypes" project by
   * Alexander Konovalov, chosen because it's devoid of boxing issues and
   * a good choice until opaque types will land in Scala.
   * [[https://github.com/alexknvl/newtypes alexknvl/newtypes]].
   *
   */
  type Par[+F[_], +A] = Par.Type[F, A]

  object Par {

    type Base
    trait Tag extends Any
    type Type[+F[_], +A] <: Base with Tag

    def apply[F[_], A](fa: Redis[F, A]): Type[F, A] =
      fa.asInstanceOf[Type[F, A]]

    def unwrap[F[_], A](fa: Type[F, A]): Redis[F, A] =
      fa.asInstanceOf[Redis[F, A]]

    def parallel[F[_]]: ({ type M[A] = Redis[F, A] })#M ~> ({ type M[A] = Par[F, A] })#M = new ~>[({ type M[A] = Redis[F, A] })#M, ({ type M[A] = Par[F, A] })#M]{
      def apply[A](fa: Redis[F,A]): Par[F,A] = Par(fa)
    }
    def sequential[F[_]]: ({ type M[A] = Par[F, A] })#M ~> ({ type M[A] = Redis[F, A] })#M = new ~>[({ type M[A] = Par[F, A] })#M, ({ type M[A] = Redis[F, A] })#M]{
      def apply[A](fa: Par[F,A]): Redis[F,A] = unwrap(fa)
    }

    implicit def parApplicative[F[_]: Parallel: Monad]: Applicative[({ type M[A] = Par[F, A] })#M] = new Applicative[({ type M[A] = Par[F, A] })#M]{
      def ap[A, B](ff: Par[F,A => B])(fa: Par[F,A]): Par[F,B] = Par(Redis(
        Par.unwrap(ff).unRedis.flatMap{ ff => 
          Par.unwrap(fa).unRedis.map{fa =>  Parallel[F].sequential(
            Parallel[F].applicative.ap(Parallel[F].parallel(ff))(Parallel[F].parallel(fa))
          )}
        }
      ))
      def pure[A](x: A): Par[F,A] = Par(Redis(
        Kleisli.pure[F, RedisConnection[F], F[A]](x.pure[F])
      ))
    }

  }

  implicit def monad[F[_]: Monad]: Monad[({ type M[A] = Redis[F, A] })#M] = new Monad[({ type M[A] = Redis[F, A] })#M]{

    def tailRecM[A, B](a: A)(f: A => Redis[F,Either[A,B]]): Redis[F,B] = Redis(
      Monad[({ type M[A] = Kleisli[F, RedisConnection[F], A]})#M].tailRecM[A, B](a)(f.andThen(_.unRedis.flatMap(fe => Kleisli.liftF(fe))))
        .map(_.pure[F])
    )

    def flatMap[A, B](fa: Redis[F,A])(f: A => Redis[F,B]): Redis[F,B] = Redis(
      fa.unRedis.flatMap(fa => 
        Kleisli.liftF[F, RedisConnection[F], A](fa)
          .flatMap(a =>
            Monad[({ type M[A] = Kleisli[F, RedisConnection[F], A]})#M].tailRecM[A, F[B]](a)(
              f.andThen(_.unRedis.flatMap(fe => Kleisli.liftF(fe.map(e => Either.right(e).map(_.pure[F])))))
            )
          )
      )
    )
    
    def pure[A](x: A): Redis[F, A] = Redis(
      Kleisli.pure[F, RedisConnection[F], F[A]](x.pure[F])
    )
  }

  implicit def parRedis[M[_]: Parallel: Concurrent]: Parallel[({ type L[A] = Redis[M, A] })#L] = new Parallel[({ type L[A] = Redis[M, A] })#L]{
    
    type F[A] = Par[M, A]
    
    def sequential: F ~> ({ type L[X] = Redis[M, X] })#L =
      Par.sequential[M]
    
    def parallel: ({ type L[A] = Redis[M, A] })#L ~> F = Par.parallel[M]
    
    def applicative: Applicative[F] = Par.parApplicative[M] 
    
    def monad: Monad[({ type L[A] = Redis[M, A] })#L] = Redis.monad[M]
  }
}