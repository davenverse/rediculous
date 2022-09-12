package io.chrisdavenport.rediculous

import cats.Contravariant
import io.chrisdavenport.rediculous.RedisProtocol.RedisType

trait RedisArg[A]{
  def encode(a: A): String
}

object RedisArg {
  def apply[A](implicit ev: RedisArg[A]): ev.type = ev

  implicit val contra : Contravariant[RedisArg] = new Contravariant[RedisArg]{
    def contramap[A, B](fa: RedisArg[A])(f: B => A): RedisArg[B] = new RedisArg[B] {
      def encode(a: B): String = fa.encode(f(a))
    }
  }

  implicit val string : RedisArg[String] = new RedisArg[String] {
    def encode(a: String): String = a
  }

  implicit val int: RedisArg[Int] = new RedisArg[Int] {
    def encode(a: Int): String = a.toString()
  }

  implicit val long: RedisArg[Long] = new RedisArg[Long] {
    def encode(a: Long): String = a.toString()
  }

  // Check this later
  implicit val double: RedisArg[Double] = new RedisArg[Double] {
    def encode(a: Double): String = 
      if (a.isInfinite() && a > 0) "+inf"
      else if (a.isInfinite() && a < 0) "-inf"
      else a.toString()
  }

  implicit val `type`: RedisArg[RedisType] = new RedisArg[RedisType] {
    def encode(a: RedisType): String = 
      a match {
        case RedisProtocol.RedisType.String => "string"
        case RedisProtocol.RedisType.Hash => "hash"
        case RedisProtocol.RedisType.List => "list"
        case RedisProtocol.RedisType.Set => "set"
        case RedisProtocol.RedisType.Stream => "stream"
        case RedisProtocol.RedisType.ZSet => "zset" 
        case _ => throw RedisError.Generic(s"Rediculous: Cannot encode $a")
      }
  }
}