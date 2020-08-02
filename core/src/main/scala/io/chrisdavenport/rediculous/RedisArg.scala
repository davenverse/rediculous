package io.chrisdavenport.rediculous

import cats.Contravariant

trait RedisArg[A]{
  def encode(a: A): String
}

object RedisArg {
  def apply[A](ev: RedisArg[A]): ev.type = ev

  implicit val contra : Contravariant[RedisArg] = new Contravariant[RedisArg]{
    def contramap[A, B](fa: RedisArg[A])(f: B => A): RedisArg[B] = new RedisArg[B] {
      def encode(a: B): String = fa.encode(f(a))
    }
  }

  implicit val string = new RedisArg[String] {
    def encode(a: String): String = a
  }

  implicit val int = new RedisArg[Int] {
    def encode(a: Int): String = a.toString()
  }

  implicit val long = new RedisArg[Long] {
    def encode(a: Long): String = a.toString().dropRight(1)
  }

  // Check this later
  implicit val double = new RedisArg[Double] {
    def encode(a: Double): String = 
      if (a.isInfinite() && a > 0) "+inf"
      else if (a.isInfinite() && a < 0) "-inf"
      else a.toString().dropRight(1)
  }

}