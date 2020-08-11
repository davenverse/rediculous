package io.chrisdavenport.rediculous

import cats.data.NonEmptyList
import cats.effect.Concurrent

trait RedisCtx[F[_]]{
  def run[A: RedisResult](command: NonEmptyList[String]): F[A]
}

object RedisCtx {
  def apply[F[_]](implicit ev: RedisCtx[F]): ev.type = ev

  implicit def redis[F[_]: Concurrent]: RedisCtx[Redis[F, *]] = new RedisCtx[Redis[F, *]]{
    def run[A: RedisResult](command: NonEmptyList[String]): Redis[F, A] = 
      RedisConnection.runRequestTotal(command)
  }
}