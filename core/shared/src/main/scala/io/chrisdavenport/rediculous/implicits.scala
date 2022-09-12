package io.chrisdavenport.rediculous

object implicits {
  implicit class RedisArgOps[A](a: A)(implicit R: RedisArg[A]){
    def encode: String = R.encode(a)
  }

  implicit class RedisResultOps(resp: Resp){
    def decode[A: RedisResult]: Either[Resp, A] = RedisResult[A].decode(resp)
  }
}