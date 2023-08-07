package io.chrisdavenport.rediculous

import cats._
import cats.syntax.all._
import scodec.bits.ByteVector

trait RedisResult[+A]{
  def decode(resp: Resp): Either[Resp, A]
}
object RedisResult extends RedisResultLowPriority{
  def apply[A](implicit ev: RedisResult[A]): ev.type = ev

  implicit val functor: Functor[RedisResult] = new Functor[RedisResult]{
    def map[A, B](fa: RedisResult[A])(f: A => B): RedisResult[B] = new RedisResult[B] {
      def decode(resp: Resp): Either[Resp,B] = fa.decode(resp).map(f)
    }
  }
  
  implicit val resp: RedisResult[Resp] = new RedisResult[Resp]{
    def decode(resp: Resp): Either[Resp,Resp] = Right(resp)
  }

  implicit val string : RedisResult[String] = new RedisResult[String]{
    def decode(resp: Resp): Either[Resp,String] = resp match {
      case Resp.SimpleString(value) => value.asRight
      case Resp.BulkString(Some(value)) => value.decodeUtf8.leftMap(_ => resp)
      case otherwise => otherwise.asLeft
    }
  }

  implicit val bytevector: RedisResult[ByteVector] = new RedisResult[ByteVector] {
    def decode(resp: Resp): Either[Resp,ByteVector] = resp match {
      case Resp.BulkString(Some(value)) => value.asRight
      case otherwise => otherwise.asLeft
    }
  }

  implicit def option[A: RedisResult]: RedisResult[Option[A]] = new RedisResult[Option[A]] {
    def decode(resp: Resp): Either[Resp,Option[A]] = resp match {
      case Resp.BulkString(None) => None.asRight
      case Resp.Array(None) => None.asRight
      case otherwise => RedisResult[A].decode(otherwise).map(_.some)
    }
  }

  implicit val status: RedisResult[RedisProtocol.Status] = new RedisResult[RedisProtocol.Status] {
    def decode(resp: Resp): Either[Resp,RedisProtocol.Status] = resp match {
      case Resp.SimpleString(value) => Either.right(value match {
        case "OK" => RedisProtocol.Status.Ok
        case "PONG" => RedisProtocol.Status.Pong
        case otherwise => RedisProtocol.Status.Status(otherwise)
      })
      case otherwise => Left(otherwise)
    }
  }

  implicit val redisType: RedisResult[RedisProtocol.RedisType] = new RedisResult[RedisProtocol.RedisType] {
    def decode(resp: Resp): Either[Resp,RedisProtocol.RedisType] = resp match {
      case Resp.SimpleString(value) => Either.right(value match {
        case "none" => RedisProtocol.RedisType.None
        case "string" => RedisProtocol.RedisType.String
        case "hash" => RedisProtocol.RedisType.Hash
        case "list" => RedisProtocol.RedisType.List
        case "set" => RedisProtocol.RedisType.Set
        case "stream" => RedisProtocol.RedisType.Stream
        case "zset" => RedisProtocol.RedisType.ZSet
        case _ => throw RedisError.Generic(s"Rediculous: Unhandled red type: $value")
      })
      case r => Left(r)
    }
  }
  
  implicit val bool: RedisResult[Boolean] = new RedisResult[Boolean] {
    def decode(resp: Resp): Either[Resp,Boolean] = resp match {
      case Resp.Integer(1) => Right(true)
      case Resp.Integer(0) => Right(false)
      case Resp.BulkString(None) => Right(false) // Lua boolean false
      case r => Left(r)
    }
  }

  implicit val long: RedisResult[Long] = new RedisResult[Long] {
    def decode(resp: Resp): Either[Resp,Long] = resp match {
      case Resp.Integer(l) => Right(l)
      case other => RedisResult[String].decode(other)
        .flatMap(s => Either.catchNonFatal(s.toLong).leftMap(_ => resp))
    }
  }

  // Increment
  implicit val double: RedisResult[Double] = new RedisResult[Double] {
    def decode(resp: Resp): Either[Resp,Double] = RedisResult[String].decode(resp)
      .flatMap(s => Either.catchNonFatal(s.toDouble).leftMap(_ => resp))
  }

  implicit val int: RedisResult[Int] = long.map(_.toInt) // Integers are longs in redis, use at your own risk.

  implicit def tuple[A: RedisResult, B: RedisResult]: RedisResult[(A, B)] = new RedisResult[(A, B)] {
    def decode(resp: Resp): Either[Resp,(A, B)] = resp match {
      case Resp.Array(Some(x ::y :: Nil)) => (RedisResult[A].decode(x), RedisResult[B].decode(y)).tupled
      case otherwise => Left(otherwise)
    }
  }

  implicit def tuple3[A: RedisResult, B: RedisResult, C: RedisResult]: RedisResult[(A, B, C)] = new RedisResult[(A, B, C)] {
    def decode(resp: Resp): Either[Resp,(A, B, C)] = resp match {
      case Resp.Array(Some(x ::y :: z :: Nil)) => (RedisResult[A].decode(x), RedisResult[B].decode(y), RedisResult[C].decode(z)).tupled
      case otherwise => Left(otherwise)
    }
  }

  implicit def tuple4[A: RedisResult, B: RedisResult, C: RedisResult, D: RedisResult]: RedisResult[(A, B, C, D)] = new RedisResult[(A, B, C, D)] {
    def decode(resp: Resp): Either[Resp,(A, B, C, D)] = resp match {
      case Resp.Array(Some(a :: b :: c :: d :: Nil)) => 
        (
          RedisResult[A].decode(a), 
          RedisResult[B].decode(b),
          RedisResult[C].decode(c),
          RedisResult[D].decode(d),
        ).tupled
      case otherwise => Left(otherwise)
    }
  }

  implicit def kv[K: RedisResult, V: RedisResult]: RedisResult[List[(K, V)]] = 
    new RedisResult[List[(K, V)]] {
      def decode(resp: Resp): Either[Resp,List[(K, V)]] = {

        def pairs(l: List[Resp]): Either[Resp,List[(K, V)]] =
          Monad[Either[Resp, *]].tailRecM[(List[Resp], List[(K, V)]), List[(K, V)]]((l, Nil)){
            case (l, acc) =>
              l match {
                case Nil => Right(Right(acc))
                case _ :: Nil => Left(resp)
                case x1 :: x2 :: xs => for {
                  k <- RedisResult[K].decode(x1)
                  v <- RedisResult[V].decode(x2)
                } yield Left((xs, (k, v) :: acc))
              }
        }.map(_.reverse)

        resp match {
          case Resp.Array(Some(rs)) => pairs(rs)
          case otherwise => Left(otherwise)
        }
      }
    }

}

private[rediculous] trait RedisResultLowPriority {

  implicit def list[A: RedisResult]: RedisResult[List[A]] = new RedisResult[List[A]] {
    def decode(resp: Resp): Either[Resp,List[A]] = resp match {
      case Resp.Array(Some(x)) => x.traverse(RedisResult[A].decode)
      case other => Left(other)
    }
  }
}
