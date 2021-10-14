package io.chrisdavenport.rediculous

import fs2._
import fs2.io.net._
import cats._
import cats.syntax.all._
import cats.effect._
import cats.effect.syntax.all._
import cats.data.NonEmptyList

trait RedisPubSub[F[_]]{
  def psubscribe(s: String, cb: Resp => F[Unit]): Resource[F, Unit]
}

object RedisPubSub {
  def socket[F[_]: Concurrent](socket: Socket[F]): RedisPubSub[F] = new RedisPubSub[F] {
    def psubscribe(s: String, cb: Resp => F[Unit]): Resource[F,Unit] = 
      Resource.make(socket.write(Chunk.array(Resp.encode(Resp.renderRequest(NonEmptyList.of("psubscribe", s))))))(
        _ => socket.write(Chunk.array(Resp.encode(Resp.renderRequest(NonEmptyList.of("punsubscribe", s))))).attempt.void
      ) *> {
        def readMessages(acc: List[List[Resp]], lastArr: Array[Byte]): F[Unit] = {
          socket.read(4096).flatMap{
            case None => 
              ApplicativeError[F, Throwable].raiseError[Unit](RedisError.Generic("Rediculous: Terminated Before reaching Equal size"))
            case Some(bytes) => 
              Resp.parseAll(lastArr.toArray ++ bytes.toIterable) match {
                case e@Resp.ParseError(_, _) => ApplicativeError[F, Throwable].raiseError[Unit](e)
                case Resp.ParseIncomplete(arr) => readMessages(acc, arr)
                case Resp.ParseComplete(value, rest) => 
                  value.traverse_(cb) *> readMessages(List.empty, rest)
                
              }
          }
        }
        readMessages(List.empty, Array()).background.void
      }

  }
}