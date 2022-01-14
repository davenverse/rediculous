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
import org.typelevel.keypool.Reusable
import scodec.bits._
import java.nio.charset.StandardCharsets
import RedisCtx.syntax.all._

/**
 * A RedisPubSub Represent an connection or group of connections
 * communicating to the pubsub subsystem of Redis.
 * 
 * Only one caller should be responsible for runMessages, but delegation of how to handle errors
 * and what to do when the connection closes or what state it closes is left to the user so they
 * can determine what to do.
 * 
 * Subscription commands are run synchronous to matching subscriptions. If your operations
 * need to take a long time please delegate them into a queue for handling without
 * holding up other messages being processed.
 **/
trait RedisPubSub[F[_]]{
  def publish(channel: String, message: String): F[Int]

  def psubscribe(s: String, cb: RedisPubSub.PubSubMessage.PMessage => F[Unit]): F[Unit]
  def psubscriptions: F[List[String]]
  def punsubscribe(s: String): F[Unit]
  def subscribe(s: String, cb: RedisPubSub.PubSubMessage.Message => F[Unit]): F[Unit]
  def subscriptions: F[List[String]]
  def unsubscribe(s: String): F[Unit]
  def unsubscribeAll: F[Unit]

  def unhandledMessages(cb: RedisPubSub.PubSubMessage => F[Unit]): F[Unit]
  def nonMessages(cb: RedisPubSub.PubSubReply => F[Unit]): F[Unit]

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
    case class Subscribed(subscription: String, subscriptionsCount: Int) extends PubSubReply
    case object Pong extends PubSubReply
    case class Unsubscribed(subscription: String, subscriptionsCount: Int) extends PubSubReply
    case class Msg(message: PubSubMessage) extends PubSubReply


    implicit val resp: RedisResult[PubSubReply] = new RedisResult[PubSubReply] {
      val emptyBV = ByteVector.empty
      val pongBV = ByteVector.encodeUtf8("pong").fold(throw _, identity)
      def decode(resp: Resp): Either[Resp,PubSubReply] = resp match {
        case r@Resp.Array(Some(Resp.BulkString(Some(pong)) :: Resp.BulkString(Some(empty)) :: Nil)) if pong == pongBV && empty == emptyBV => 
          Pong.asRight
        case r@Resp.Array(Some(r0 :: r1 :: r2 :: rs)) => 
          r0.decode[String].flatMap{
            case "message" => 
              (r1.decode[String], r2.decode[String])
                .mapN{ case (channel, message) => Msg(PubSubMessage.Message(channel, message))}
            case "pmessage" => 
              (r1.decode[String], r2.decode[String], rs.headOption.toRight(r).flatMap(_.decode[String]))
                .mapN{ case (pattern, channel, message) => Msg(PubSubMessage.PMessage(pattern, channel, message))}
            case "subscribe" => 
              (r1.decode[String], r2.decode[Int]).mapN(Subscribed(_, _))
            case "psubscribe" => 
              (r1.decode[String], r2.decode[Int]).mapN(Subscribed(_, _))
            case "unsubscribe" => 
              (r1.decode[String], r2.decode[Int]).mapN(Unsubscribed(_, _))
            case "punsubscribe" =>
              (r1.decode[String], r2.decode[Int]).mapN(Unsubscribed(_, _))
            case _ => r.asLeft
          }
        case otherwise => otherwise.asLeft
      }
    }
  }

  private def socket[F[_]: Concurrent](connection: RedisConnection[F], sockets: List[Socket[F]], maxBytes: Int, onNonMessage: Ref[F, PubSubReply => F[Unit]], onUnhandledMessage: Ref[F, PubSubMessage => F[Unit]], cbStorage: Ref[F, Map[String, PubSubMessage => F[Unit]]]): RedisPubSub[F] = new RedisPubSub[F] {
    val subPrefix = "cs:"
    val pSubPrefix = "ps:"

    def publish(s: String, message: String): F[Int] = 
      RedisCtx[({type M[A] = Redis[F, A]})#M].unkeyed[Int](cats.data.NonEmptyList.of("PUBLISH", s, message)).run(connection)

    def unsubscribeAll: F[Unit] = cbStorage.get.map(_.keys.toList).flatMap{list => 
      val channelSubscriptions = list.collect{ case x if x.startsWith("c") => x.drop(3)}
      val patternSubscriptions = list.collect{ case x if x.startsWith("p") => x.drop(3)}
      channelSubscriptions.traverse_(unsubscribe) >>
      patternSubscriptions.traverse_(punsubscribe)
    }

    def subscriptions: F[List[String]] = {
      cbStorage.get.map(_.keys.toList).map{list => 
        list.collect{ case x if x.startsWith("c") => x.drop(3)}
      }
    }

    def psubscriptions: F[List[String]] = {
      cbStorage.get.map(_.keys.toList).map{list => 
        list.collect{ case x if x.startsWith("p") => x.drop(3)}
      }
    }

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

    def readMessages(socket: Socket[F], lastArr: Array[Byte]): F[Unit] = {
      socket.reads.through(fs2.interop.scodec.StreamDecoder.many(Resp.CodecUtils.codec).toPipeByte)
        .chunks
        .evalMap(chunkResp => 
          cbStorage.get.flatMap{cbmap => 
            chunkResp.traverse{ resp => 
              PubSubReply.resp.decode(resp)
                .leftMap(resp => RedisError.Generic(s"Rediculous: Not PubSubReply Response Type got: $resp"))
                .liftTo[F]
                .flatMap{
                  case PubSubReply.Msg(p@PubSubMessage.PMessage(pattern, _, _)) => cbmap.get(pSubPrefix + pattern).map(f => f(p)).getOrElse(onUnhandledMessage.get.flatMap(f => f(p)))
                  case PubSubReply.Msg(p@PubSubMessage.Message(channel, _)) => cbmap.get(subPrefix + channel).map(f => f(p)).getOrElse(onUnhandledMessage.get.flatMap(f => f(p)))
                  case other => onNonMessage.get.flatMap(f => f(other))
                }
            }
          }
        ).compile.drain
    }

    def unhandledMessages(cb: RedisPubSub.PubSubMessage => F[Unit]): F[Unit] = onUnhandledMessage.set(cb)
    def nonMessages(cb: RedisPubSub.PubSubReply => F[Unit]): F[Unit] = onNonMessage.set(cb)

    private def encodeResp(nel: NonEmptyList[String]): F[Chunk[Byte]] = {
      val resp = Resp.renderRequest(nel.map(ByteVector.encodeUtf8(_).fold(throw _, identity(_))))
      Resp.CodecUtils.codec.encode(resp)
        .toEither
        .leftMap(err => new RuntimeException(s"Encoding Error - $err"))
        .map(bits => Chunk.byteVector(bits.bytes))
        .liftTo[F]
    }

    def psubscribe(s: String, cb: PubSubMessage.PMessage => F[Unit]): F[Unit] = 
      addPSubscribe(s, cb).ifM(
        encodeResp(NonEmptyList.of("psubscribe", s)).flatMap(chunk => sockets.traverse_(_.write(chunk))),
        Applicative[F].unit
      )

    def punsubscribe(s: String): F[Unit] = 
      encodeResp(NonEmptyList.of("punsubscribe", s)).flatMap(chunk => sockets.traverse_(_.write(chunk))) >>
      removePSubscribe(s)

    def subscribe(s: String, cb: PubSubMessage.Message => F[Unit]): F[Unit] = 
      addSubscribe(s, cb).ifM(
        encodeResp(NonEmptyList.of("subscribe", s)).flatMap(chunk => sockets.traverse_(_.write(chunk))),
        Applicative[F].unit
      )
      
    def unsubscribe(s: String) = 
      encodeResp(NonEmptyList.of("unsubscribe", s)).flatMap(chunk => sockets.traverse_(_.write(chunk))) >> 
      removeSubscribe(s)

    def ping: F[Unit] = encodeResp(NonEmptyList.of("ping")).flatMap(chunk => sockets.traverse_(_.write(chunk)))
  
    def runMessages: F[Unit] = sockets match {
      case Nil => 
        Applicative[F].unit
      case head :: Nil =>
        readMessages(head, Array())
      case otherwise => 
        Stream.emits(otherwise)
        .covary[F]
        .parEvalMap(Int.MaxValue)(readMessages(_, Array()))
        .compile
        .drain
    }
  }


  /**
   * Create a RedisPubSub Connection from a connection.
   * 
   * Cluster Broadcast is used for keyspace notifications which are only local to the node so require
   * connections to all nodes.
   **/
  def fromConnection[F[_]: Concurrent](connection: RedisConnection[F], maxBytes: Int = 8096, clusterBroadcast: Boolean = false): Resource[F, RedisPubSub[F]] = connection match {
    case RedisConnection.Queued(_, sockets) => 
      sockets.flatMap{managed => 
        val messagesR = Concurrent[F].ref(Map[String, RedisPubSub.PubSubMessage => F[Unit]]())
        val onNonMessageR = Concurrent[F].ref((_: PubSubReply) => Applicative[F].unit)
        val onUnhandledMessageR = Concurrent[F].ref((_: PubSubMessage) => Applicative[F].unit)
        Resource.eval((messagesR, onNonMessageR, onUnhandledMessageR).tupled).flatMap{case (ref, onNonMessage, onUnhandledMessage) => 
          Resource.makeCase(socket(connection, managed.value :: Nil, maxBytes, onNonMessage, onUnhandledMessage, ref).pure[F]){
            case (_, Resource.ExitCase.Errored(_)) | (_, Resource.ExitCase.Canceled) => managed.canBeReused.set(Reusable.DontReuse)
            case (pubsub, Resource.ExitCase.Succeeded) =>  pubsub.unsubscribeAll
          }
        }
      }
    case RedisConnection.PooledConnection(pool) => 
      pool.take(()).map(_.map(_._1)).flatMap{managed => 
        val messagesR = Concurrent[F].ref(Map[String, RedisPubSub.PubSubMessage => F[Unit]]())
        val onNonMessageR = Concurrent[F].ref((_: PubSubReply) => Applicative[F].unit)
        val onUnhandledMessageR = Concurrent[F].ref((_: PubSubMessage) => Applicative[F].unit)
        Resource.eval((messagesR, onNonMessageR, onUnhandledMessageR).tupled).flatMap{case (ref, onNonMessage, onUnhandledMessage) => 
          Resource.makeCase(socket(connection, managed.value :: Nil, maxBytes, onNonMessage, onUnhandledMessage, ref).pure[F]){
            case (_, Resource.ExitCase.Errored(_)) | (_, Resource.ExitCase.Canceled) => managed.canBeReused.set(Reusable.DontReuse)
            case (pubsub, Resource.ExitCase.Succeeded) =>  pubsub.unsubscribeAll
          }
        }
      }
    case RedisConnection.DirectConnection(s) => 
      val messagesR = Concurrent[F].ref(Map[String, RedisPubSub.PubSubMessage => F[Unit]]())
      val onNonMessageR = Concurrent[F].ref((_: PubSubReply) => Applicative[F].unit)
      val onUnhandledMessageR = Concurrent[F].ref((_: PubSubMessage) => Applicative[F].unit)
      Resource.eval((messagesR, onNonMessageR, onUnhandledMessageR).tupled).flatMap{case (ref, onNonMessage, onUnhandledMessage) => 
      Resource.make(socket(connection, s :: Nil, maxBytes, onNonMessage, onUnhandledMessage, ref).pure[F]){
        pubsub => pubsub.unsubscribeAll
      }
      }
    case RedisConnection.Cluster(_, topology, sockets) => 
      val messagesR = Concurrent[F].ref(Map[String, RedisPubSub.PubSubMessage => F[Unit]]())
      val onNonMessageR = Concurrent[F].ref((_: PubSubReply) => Applicative[F].unit)
      val onUnhandledMessageR = Concurrent[F].ref((_: PubSubMessage) => Applicative[F].unit)
      Resource.eval((messagesR, onNonMessageR, onUnhandledMessageR).tupled).flatMap{case (ref, onNonMessage, onUnhandledMessage) => 
        Resource.eval(topology).flatMap{slots => 
          val servers = slots.l.flatMap(slot => slot.replicas.map(server => (server.host, server.port))).distinct
          val usedServers = if (clusterBroadcast) servers else servers.head :: Nil
          val usedSockets: Resource[F, List[org.typelevel.keypool.Managed[F, Socket[F]]]] = usedServers.traverse{ case (host, port) => sockets(host, port) }
          usedSockets.flatMap(list => 
            Resource.makeCase(socket(connection, list.map(_.value), maxBytes, onNonMessage, onUnhandledMessage, ref).pure[F]){
              case (_, Resource.ExitCase.Errored(_)) | (_, Resource.ExitCase.Canceled) => list.traverse_(managed => managed.canBeReused.set(Reusable.DontReuse))
              case (pubsub, Resource.ExitCase.Succeeded) =>  pubsub.unsubscribeAll
            }
          )
        }
      }
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