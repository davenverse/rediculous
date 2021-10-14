package io.chrisdavenport.rediculous

import fs2._
import fs2.io.net._
import cats._
import cats.syntax.all._
import cats.effect._
import cats.effect.syntax.all._
import cats.data.NonEmptyList
import scala.concurrent.duration._
import _root_.io.chrisdavenport.rediculous.implicits._

trait RedisPubSub[F[_]]{
  def psubscribe(s: String, cb: RedisPubSub.PubSubMessage.PMessage => F[Unit]): F[Unit]
  def punsubscribe(s: String): F[Unit] 
  def subscribe(s: String, cb: RedisPubSub.PubSubMessage.Message => F[Unit]): F[Unit]
  def unsubscribe(s: String): F[Unit]
  def ping: F[Unit]

  def runMessages: F[Unit]
}

object RedisPubSub {

  sealed trait PubSubMessage
  object PubSubMessage {
    case class Message(channel: String, message: String) extends PubSubMessage
    case class PMessage(pattern: String, channel: String, message: String) extends PubSubMessage
  }
  sealed trait PubSubReply
  object PubSubReply {
    case object Subscribed extends PubSubReply
    case object Pong extends PubSubReply
    case class Unsubscribed(subscriptionsLeft: Int) extends PubSubReply
    case class Msg(message: PubSubMessage) extends PubSubReply

    implicit val resp: RedisResult[PubSubReply] = new RedisResult[PubSubReply] {
      def decode(resp: Resp): Either[Resp,PubSubReply] = resp match {
        case r@Resp.Array(Some(Resp.BulkString(Some("pong")) :: Resp.BulkString(Some("")) :: Nil)) => 
          Pong.asRight
        case r@Resp.Array(Some(r0 :: r1 :: r2 :: rs)) => 
          r0.decode[String].flatMap{
            case "message" => 
              (r1.decode[String], r2.decode[String])
                .mapN{ case (channel, message) => Msg(PubSubMessage.Message(channel, message))}
            case "pmessage" => 
              (r1.decode[String], r2.decode[String], rs.headOption.toRight(r).flatMap(_.decode[String]))
                .mapN{ case (pattern, channel, message) => Msg(PubSubMessage.PMessage(pattern, channel, message))}
            case "subscribe" => Subscribed.asRight
            case "psubscribe" => Subscribed.asRight
            case "unsubscribe" => 
              r2.decode[Int].map(Unsubscribed(_))
            case "punsubscribe" =>
              r2.decode[Int].map(Unsubscribed(_))
            case _ => r.asLeft
          }
        case otherwise => otherwise.asLeft
      }
    }
  }

  def socket[F[_]: Concurrent](socket: Socket[F], onNonMessage: PubSubReply => F[Unit], cbStorage: Ref[F, Map[String, PubSubMessage => F[Unit]]]): RedisPubSub[F] = new RedisPubSub[F] {
    val subPrefix = "subscribed:"
    val pSubPrefix = "psubscribed:"

    def addSubscribe(key: String, effect: PubSubMessage.Message => F[Unit]): F[Boolean] = {
      val usedKey = subPrefix + key
      val widened: PubSubMessage => F[Unit] = {
        case m: PubSubMessage.Message => effect(m)
        case _ => Applicative[F].unit
      }
      cbStorage.modify{m => 
        val wasEmpty = m.get(usedKey).isEmpty
        val out = m.+(usedKey -> widened)
        (out, wasEmpty)
      }
    }
    def removeSubscribe(key: String) = {
      cbStorage.update(_ - (subPrefix + key))
    }

    def addPSubscribe(key: String, effect: PubSubMessage.PMessage => F[Unit]): F[Boolean] = {
      val usedKey = pSubPrefix + key
      val widened: PubSubMessage => F[Unit] = {
        case m: PubSubMessage.PMessage => effect(m)
        case _ => Applicative[F].unit
      }
      cbStorage.modify{m => 
        val wasEmpty = m.get(usedKey).isEmpty
        val out = m.+(usedKey -> widened)
        (out, wasEmpty)
      }
    }

    def removePSubscribe(key: String) = {
      cbStorage.update(_ - (pSubPrefix + key))
    }

    def readMessages(lastArr: Array[Byte]): F[Unit] = {
      socket.read(4096).flatMap{
        case None => 
          ApplicativeError[F, Throwable].raiseError[Unit](RedisError.Generic("Rediculous: Connection Closed"))
        case Some(bytes) => 
          Resp.parseAll(lastArr.toArray ++ bytes.toIterable) match {
            case e@Resp.ParseError(_, _) => ApplicativeError[F, Throwable].raiseError[Unit](e)
            case Resp.ParseIncomplete(arr) => readMessages(arr)
            case Resp.ParseComplete(value, rest) => cbStorage.get.flatMap{cbmap => 
              value.traverse_(resp => 
                PubSubReply.resp.decode(resp)
                  .leftMap(resp => RedisError.Generic(s"Rediculous: Not PubSubReply Response Type got: $resp"))
                  .liftTo[F]
                  .flatMap{
                    case PubSubReply.Msg(p@PubSubMessage.PMessage(pattern, _, _)) => cbmap.get(pSubPrefix + pattern).traverse_(f => f(p))
                    case PubSubReply.Msg(p@PubSubMessage.Message(channel, _)) => cbmap.get(subPrefix + channel).traverse_(f => f(p))
                    case other => onNonMessage(other)
                  }
              )
            } *> readMessages(rest)
          }
      }
    }

    def psubscribe(s: String, cb: PubSubMessage.PMessage => F[Unit]): F[Unit] = 
      addPSubscribe(s, cb).ifM(
        socket.write(Chunk.array(Resp.encode(Resp.renderRequest(NonEmptyList.of("psubscribe", s))))),
        Applicative[F].unit
      )

    def punsubscribe(s: String): F[Unit] = 
      socket.write(Chunk.array(Resp.encode(Resp.renderRequest(NonEmptyList.of("punsubscribe", s))))) >>
      removePSubscribe(s)

    def subscribe(s: String, cb: PubSubMessage.Message => F[Unit]): F[Unit] = 
      addSubscribe(s, cb).ifM(
        socket.write(Chunk.array(Resp.encode(Resp.renderRequest(NonEmptyList.of("subscribe", s))))),
        Applicative[F].unit
      )
      
    def unsubscribe(s: String) = 
      socket.write(Chunk.array(Resp.encode(Resp.renderRequest(NonEmptyList.of("unsubscribe", s))))) >> 
      removeSubscribe(s)

    def ping: F[Unit] = socket.write(Chunk.array(Resp.encode(Resp.renderRequest(NonEmptyList.of("ping")))))
  
    def runMessages: F[Unit] = 
      readMessages(Array())

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