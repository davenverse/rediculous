package io.chrisdavenport.rediculous

import fs2._
import fs2.io.net._
import cats._
import cats.syntax.all._
import cats.effect._
import cats.effect.syntax.all._
import cats.data.NonEmptyList
import _root_.io.chrisdavenport.rediculous.RedisPubSub.Message
import scala.concurrent.duration._

trait RedisPubSub[F[_]]{
  def psubscribe(s: String, cb: Resp => F[Unit]): Resource[F, Unit]
  def subscribe(s: String, cb: Resp => F[Unit]): Resource[F, Unit]
}

object RedisPubSub {
  def socket[F[_]: Temporal](socket: Socket[F]): RedisPubSub[F] = new RedisPubSub[F] {
    def readMessages(acc: List[List[Resp]], lastArr: Array[Byte], cb: Resp => F[Unit]): F[Unit] = {
          socket.read(4096).flatMap{
            case None => 
              ApplicativeError[F, Throwable].raiseError[Unit](RedisError.Generic("Rediculous: Terminated Before reaching Equal size"))
            case Some(bytes) => 
              Resp.parseAll(lastArr.toArray ++ bytes.toIterable) match {
                case e@Resp.ParseError(_, _) => ApplicativeError[F, Throwable].raiseError[Unit](e)
                case Resp.ParseIncomplete(arr) => readMessages(acc, arr, cb)
                case Resp.ParseComplete(value, rest) => 
                  value.traverse_(cb) *> readMessages(List.empty, rest, cb)
              }
          }
        }


    def psubscribe(s: String, cb: Resp => F[Unit]): Resource[F,Unit] = 
      Resource.make(socket.write(Chunk.array(Resp.encode(Resp.renderRequest(NonEmptyList.of("psubscribe", s))))))(
        _ => socket.write(Chunk.array(Resp.encode(Resp.renderRequest(NonEmptyList.of("punsubscribe", s))))).attempt.void
      ) *> {
        readMessages(List.empty, Array(), cb).background.void
      }  *>       
      Resource.make(socket.write(Chunk.array(Resp.encode(Resp.renderRequest(NonEmptyList.of("psubscribe", s + "s"))))))(
        _ => socket.write(Chunk.array(Resp.encode(Resp.renderRequest(NonEmptyList.of("punsubscribe", s + "s"))))).attempt.void *> Temporal[F].sleep(1.second)
      )

    def subscribe(s: String, cb: Resp => F[Unit]): Resource[F,Unit] = 
      Resource.make(socket.write(Chunk.array(Resp.encode(Resp.renderRequest(NonEmptyList.of("subscribe", s))))))(
        _ => socket.write(Chunk.array(Resp.encode(Resp.renderRequest(NonEmptyList.of("unsubscribe", s))))).attempt.void
      ) *> {
        
        readMessages(List.empty, Array(), cb).background.void
      }

  }

  // Don't like this
  sealed trait Message
  object Message {
    case class Message(channel: String, message: String) extends RedisPubSub.Message

    case class PMessage(pattern: String, channel: String, message: String) extends RedisPubSub.Message
  }

  // Subscribed Commands Allowed
  // SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE, PUNSUBSCRIBE, PING and QUIT.

  /*
  Valid Message Types
  Array(Some(List(BulkString(Some(psubscribe)), BulkString(Some(foo)), Integer(1))))
  Array(Some(List(BulkString(Some(pmessage)), BulkString(Some(foo)), BulkString(Some(foo)), BulkString(Some(hi there!))))
  Array(Some(List(BulkString(Some(subscribe)), BulkString(Some(foo)), Integer(1))))
  Array(Some(List(BulkString(Some(unsubscribe)), BulkString(Some(foos)), Integer(1))))
  Array(Some(List(BulkString(Some(message)), BulkString(Some(foo)), BulkString(Some(hi there!)))))
  Array(Some(List(BulkString(Some(punsubscribe)), BulkString(Some(foos)), Integer(1))))
  */
}