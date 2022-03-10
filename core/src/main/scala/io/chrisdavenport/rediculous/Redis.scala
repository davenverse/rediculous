package io.chrisdavenport.rediculous

import cats.data._
import cats.implicits._
import cats._
import cats.effect._

final case class Redis[F[_], A](unRedis: Kleisli[F, RedisConnection[F], A]){
  // RedisConnection[F] => F[F[A]]
  def run(connection: RedisConnection[F])(implicit ev: Monad[F]): F[A] = {
    Redis.runRedis(this)(connection)
  }
}
object Redis {

  private def runRedis[F[_]: Monad, A](redis: Redis[F, A])(connection: RedisConnection[F]): F[A] = {
    redis.unRedis.run(connection)
  }

  def liftF[F[_]: Monad, A](fa: F[A]): Redis[F, A] = 
    Redis(Kleisli.liftF[F, RedisConnection[F], A](fa))

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

    def parallel[F[_]]: Redis[F, *] ~> Par[F, *] = new ~>[Redis[F, *], Par[F, *]]{
      def apply[A](fa: Redis[F,A]): Par[F,A] = Par(fa)
    }
    def sequential[F[_]]: Par[F, *] ~> Redis[F, *] = new ~>[Par[F, *], Redis[F, *]]{
      def apply[A](fa: Par[F,A]): Redis[F,A] = unwrap(fa)
    }

    implicit def parApplicative[F[_]: Parallel: Monad]: Applicative[Par[F, *]] = new Applicative[Par[F, *]]{
      def ap[A, B](ff: Par[F,A => B])(fa: Par[F,A]): Par[F,B] = Par(Redis{
        val kfx : Kleisli[F, RedisConnection[F], A => B] = Par.unwrap(ff).unRedis
        val kfa : Kleisli[F, RedisConnection[F], A] = Par.unwrap(fa).unRedis
        kfx.parAp(kfa)
      })
      def pure[A](x: A): Par[F,A] = Par(Redis(
        Kleisli.pure[F, RedisConnection[F], A](x)
      ))
    }

  }

  implicit def monad[F[_]: Monad]: Monad[Redis[F, *]] = new Monad[Redis[F, *]]{

    def tailRecM[A, B](a: A)(f: A => Redis[F,Either[A,B]]): Redis[F,B] = Redis(
      Monad[Kleisli[F, RedisConnection[F], *]].tailRecM[A, B](a)(f.andThen(_.unRedis.flatMap(fe => Kleisli.liftF(fe.pure[F]))))
    )

    def flatMap[A, B](fa: Redis[F,A])(f: A => Redis[F,B]): Redis[F,B] = Redis(
      fa.unRedis.flatMap(f(_).unRedis)
    )

    
    def pure[A](x: A): Redis[F, A] = Redis(
      Kleisli.pure[F, RedisConnection[F], A](x)
    )
  }

  implicit def parRedis[M[_]: Parallel: Concurrent]: Parallel[Redis[M, *]] = new Parallel[Redis[M, *]]{
    
    type F[A] = Par[M, A]
    
    def sequential: F ~> Redis[M, *] =
      Par.sequential[M]
    
    def parallel: Redis[M, *] ~> F = Par.parallel[M]
    
    def applicative: Applicative[F] = Par.parApplicative[M] 
    
    def monad: Monad[Redis[M, *]] = Redis.monad[M]
  }
}