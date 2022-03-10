package io.chrisdavenport.rediculous.cluster

import cats.implicits._
import io.chrisdavenport.rediculous._
import cats.data.NonEmptyList
import cats.effect._
import com.comcast.ip4s._
import scodec.bits.ByteVector
import RedisCtx.syntax.all._

object ClusterCommands {

  final case class ClusterServer(host: Host, port: Port, id: String)
  object ClusterServer {
    implicit val result: RedisResult[ClusterServer] = new RedisResult[ClusterServer]{
      def decode(resp: Resp): Either[Resp,ClusterServer] = {
        def decodeBVString(bv: ByteVector): Either[Resp, String] = bv.decodeUtf8.leftMap(_ => resp)
        resp match {
          case Resp.Array(Some(Resp.BulkString(Some(hostBV)) :: Resp.Integer(l) :: Resp.BulkString(Some(idBV)) :: Nil)) => 
            for {
              hostS <- decodeBVString(hostBV)
              host <- Host.fromString(hostS).toRight(resp)
              port <- Port.fromInt(l.toInt).toRight(resp)
              id <- decodeBVString(idBV)
            } yield ClusterServer(host, port, id)
          case e => e.asLeft
        }
      }
    }
  }
  final case class ClusterSlot(start: Int, end: Int, replicas: List[ClusterServer])
  object ClusterSlot {
    
    implicit val result: RedisResult[ClusterSlot] = new RedisResult[ClusterSlot]{
      def decode(resp: Resp): Either[Resp,ClusterSlot] = resp match {
        case Resp.Array(Some(Resp.Integer(start) :: Resp.Integer(end) :: rest)) => 
          rest.traverse(RedisResult[ClusterServer].decode).map{l => 
            ClusterSlot(start.toInt, end.toInt, l)
          }
        case other => other.asLeft
      }
    }
  }
  final case class ClusterSlots(l: List[ClusterSlot]){
    def served(bucket: Int): Option[(Host, Port)] = 
      l.collectFirst{
        case ClusterSlot(start, end, master :: _) if start <= bucket && end >= bucket => (master.host, master.port) 
      }
    def random[F[_]: Sync] = Sync[F].delay{
      val base = l.flatMap(_.replicas)
      if (base.size > 0) {
        val i = scala.util.Random.nextInt(base.size)
        val server = base(i)
        Some((server.host, server.port))
      } else None
    }.flatMap{
      case Some(a) => Sync[F].pure(a)
      case None => Sync[F].raiseError[(Host, Port)](RedisError.Generic("Rediculous: No Servers Available"))
    }
  }
  object ClusterSlots {
    implicit val result: RedisResult[ClusterSlots] = new RedisResult[ClusterSlots]{
      def decode(resp: Resp): Either[Resp,ClusterSlots] = 
        RedisResult[List[ClusterSlot]].decode(resp).map(ClusterSlots(_))
    }
  }

  def clusterslots[F[_]: RedisCtx]: F[ClusterSlots] = 
    RedisCtx[F].unkeyed(NonEmptyList.of("CLUSTER", "SLOTS"))

}