package io.chrisdavenport

import cats.effect.IO

package object rediculous {
  type RedisIO[A] = Redis[IO, A]
}
