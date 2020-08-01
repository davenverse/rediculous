package io.chrisdavenport.rediculous

trait RedisResult[+A]{
  def decode(resp: Resp): Either[Resp, A]
}
object RedisResult{
  def apply[A](implicit ev: RedisResult[A]): ev.type = ev
  
  implicit val resp: RedisResult[Resp] = new RedisResult[Resp]{
    def decode(resp: Resp): Either[Resp,Resp] = Right(resp)
  }
}
