package io.chrisdavenport.rediculous

import cats.data.NonEmptyList
import cats.effect.Concurrent
import scala.annotation.implicitNotFound
import scodec.bits.ByteVector

/**
  * RedisCtx is the Context in Which RedisOperations operate.
  */
@implicitNotFound("""Cannot find implicit value for  RedisCtx[${F}]. 
If you are trying to build a ({ type M[A] = Redis[F, A] })#M, make sure a Concurrent[F] is in scope,
other instances are also present such as RedisTransaction.
If you are leveraging a custom context not provided by rediculous,
please consult your library documentation.
""")
trait RedisCtx[F[_]]{
  def keyedBV[A: RedisResult](key: ByteVector, command: NonEmptyList[ByteVector]): F[A]
  def unkeyedBV[A: RedisResult](command: NonEmptyList[ByteVector]): F[A]
}

object RedisCtx {
  
  def apply[F[_]](implicit ev: RedisCtx[F]): ev.type = ev

  object syntax {
    object all extends StringSyntax

    trait StringSyntax {
      implicit class RedisContext[F[_]](private val ctx: RedisCtx[F]){
        private def encodeUnsafe(s: String): ByteVector = ByteVector.encodeUtf8(s).fold(throw _, identity(_))
        // UTF8 String
        def keyed[A: RedisResult](key: String, command: NonEmptyList[String]): F[A] = {
          val k = encodeUnsafe(key)
          val c = command.map(encodeUnsafe)
          ctx.keyedBV(k, c)
        }
        def unkeyed[A: RedisResult](command: NonEmptyList[String]): F[A] = {
          val c = command.map(encodeUnsafe(_))
          ctx.unkeyedBV(c)
        }
      }
    }
  }

  implicit def redis[F[_]: Concurrent]: RedisCtx[Redis[F, *]] = new RedisCtx[Redis[F, *]]{
    def keyedBV[A: RedisResult](key: ByteVector, command: NonEmptyList[ByteVector]): Redis[F,A] =
      RedisConnection.runRequestTotal(command, Some(key))
    def unkeyedBV[A: RedisResult](command: NonEmptyList[ByteVector]): Redis[F, A] = 
      RedisConnection.runRequestTotal(command, None)
  }
}