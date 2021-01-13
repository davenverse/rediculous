package io.chrisdavenport.rediculous

import cats.Contravariant

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

}