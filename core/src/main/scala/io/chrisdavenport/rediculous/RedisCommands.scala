package io.chrisdavenport.rediculous

import cats._
import cats.implicits._
import cats.data.{NonEmptyList => NEL}
import RedisProtocol._
import _root_.io.chrisdavenport.rediculous.implicits._
import scala.collection.immutable.Nil
import RedisCtx.syntax.all._
import scodec.bits.ByteVector

object RedisCommands {

  def objectrefcount[F[_]: RedisCtx](key: String): F[Long] = 
    RedisCtx[F].keyed(key, NEL.of("OBJECT", "refcount", key.encode))

  def objectidletime[F[_]: RedisCtx](key: String): F[Long] = 
    RedisCtx[F].keyed(key, NEL.of("OBJECT", "idletime", key.encode))

  def objectencoding[F[_]: RedisCtx](key: String): F[String] =
    RedisCtx[F].keyed(key, NEL.of("OBJECT", "encoding", key.encode))

  def linsertbefore[F[_]: RedisCtx](key: String, pivot: String, value: String): F[Long] = 
    RedisCtx[F].keyed(key, NEL.of("LINSERT", key.encode, "BEFORE", pivot.encode, value.encode))

  def linsertafter[F[_]: RedisCtx](key: String, pivot: String, value: String): F[Long] = 
    RedisCtx[F].keyed(key, NEL.of("LINSERT", key.encode, "AFTER", pivot.encode, value.encode))

  def getType[F[_]: RedisCtx](key: String): F[RedisType] = 
    RedisCtx[F].keyed(key, NEL.of("TYPE", key.encode))

  // TODO slow log commands

  def zrange[F[_]: RedisCtx](key: String, start: Long, stop: Long): F[List[String]] = 
    RedisCtx[F].keyed(key, NEL.of("ZRANGE", key.encode, start.encode, stop.encode))

  def zrangewithscores[F[_]: RedisCtx](key: String, start: Long, stop: Long): F[List[(String, Double)]] = 
    RedisCtx[F].keyed(key, NEL.of("ZRANGE", key.encode, start.encode, stop.encode, "WITHSCORES"))

  def zrevrange[F[_]: RedisCtx](key: String, start: Long, stop: Long): F[List[String]] = 
    RedisCtx[F].keyed(key, NEL.of("ZREVRANGE", key.encode, start.encode, stop.encode))

  def zrevrangewithscores[F[_]: RedisCtx](key: String, start: Long, stop: Long): F[List[(String, Double)]] = 
    RedisCtx[F].keyed(key, NEL.of("ZREVRANGE", key.encode, start.encode, stop.encode, "WITHSCORES"))

  def zrangebyscore[F[_]: RedisCtx](key: String, min: Double, max: Double): F[List[String]] = 
    RedisCtx[F].keyed(key, NEL.of("ZRANGEBYSCORE", key.encode, min.encode, max.encode))

  def zrangebyscorewithscores[F[_]: RedisCtx](key: String, min: Double, max: Double): F[List[(String, Double)]] = 
    RedisCtx[F].keyed(key, NEL.of("ZRANGEBYSCORE", key.encode, min.encode, max.encode, "WITHSCORES"))

  def zrangebyscorelimit[F[_]: RedisCtx](key: String, min: Double, max: Double, offset: Long, count: Long): F[List[String]] =
    RedisCtx[F].keyed(key, NEL.of("ZRANGEBYSCORE", key.encode, min.encode, max.encode, "LIMIT", offset.encode, count.encode))

  def zrangebyscorelimitwithscores[F[_]: RedisCtx](key: String, min: Double, max: Double, offset: Long, count: Long): F[List[(String, Double)]] =
    RedisCtx[F].keyed(key, NEL.of("ZRANGEBYSCORE", key.encode, min.encode, max.encode, "WITHSCORES", "LIMIT", offset.encode, count.encode))

  def zrevrangebyscore[F[_]: RedisCtx](key: String, min: Double, max: Double): F[List[String]] = 
    RedisCtx[F].keyed(key, NEL.of("ZREVRANGEBYSCORE", key.encode, min.encode, max.encode))

  def zrevrangebyscorewithscores[F[_]: RedisCtx](key: String, min: Double, max: Double): F[List[(String, Double)]] =
    RedisCtx[F].keyed(key, NEL.of("ZREVRANGEBYSCORE", key.encode, min.encode, max.encode, "WITHSCORES"))

  def zrevrangebyscorelimit[F[_]: RedisCtx](key: String, min: Double, max: Double, offset: Long, count: Long): F[List[String]] = 
    RedisCtx[F].keyed(key, NEL.of("ZREVRANGEBYSCORE", key.encode, min.encode, max.encode, "LIMIT", offset.encode, count.encode))

  def zrevrangebyscorelimitwithscores[F[_]: RedisCtx](key: String, min: Double, max: Double, offset: Long, count: Long): F[List[(String, Double)]] = 
    RedisCtx[F].keyed(key, NEL.of("ZREVRANGEBYSCORE", key.encode, min.encode, max.encode, "WITHSCORES", "LIMIT", offset.encode, count.encode))

  // TODO Sort
  // TODO aggregate

  def eval[F[_]: RedisCtx, A: RedisResult](script: String, keys: List[String], args: List[String]): F[A] = {
    val cmd = NEL("EVAL", script :: keys.length.encode :: keys ::: args)
    keys match {
      case Nil => RedisCtx[F].unkeyed(cmd)
      case k :: _ => RedisCtx[F].keyed(k, cmd)
    }
  }
    
  def evalsha[F[_]: RedisCtx, A: RedisResult](script: String, keys: List[String], args: List[String]): F[A] = {
    val cmd = NEL("EVALSHA", script :: keys.length.encode :: keys ::: args)
    keys match {
      case Nil => RedisCtx[F].unkeyed(cmd)
      case k :: _ => RedisCtx[F].keyed(k, cmd)
    }
  }

  def bitcount[F[_]: RedisCtx](key: String): F[Long] = 
    RedisCtx[F].keyed(key, NEL.of("BITCOUNT", key.encode))

  def bitcountrange[F[_]: RedisCtx](key: String, start: Long, end: Long): F[Long] = 
    RedisCtx[F].keyed(key, NEL.of("BITCOUNT", key.encode, start.encode, end.encode))


  private def bitop[F[_]: RedisCtx](operation: String, keys: List[String]): F[Long]= {
    val cmd = NEL("BITOP", operation :: keys)
    keys match {
      case Nil => RedisCtx[F].unkeyed(cmd)
      case k :: _ => RedisCtx[F].keyed(k, cmd)
    }
  }
  
  def bitopand[F[_]: RedisCtx](destkey: String, srckeys: List[String]): F[Long] = 
    bitop("AND", destkey :: srckeys)

  def bitopor[F[_]: RedisCtx](destkey: String, srckeys: List[String]): F[Long] = 
    bitop("OR", destkey :: srckeys)

  def bitopxor[F[_]: RedisCtx](destkey: String, srckeys: List[String]): F[Long] = 
    bitop("XOR", destkey :: srckeys)

  def bitopnot[F[_]: RedisCtx](destkey: String, srckey: String): F[Long] = 
    bitop("NOT", destkey :: srckey :: Nil)

  // TODO Migrate

  sealed trait Condition
  object Condition {
    case object Nx extends Condition
    case object Xx extends Condition
    implicit val arg: RedisArg[Condition] = RedisArg[String].contramap[Condition]{
      case Nx => "NX"
      case Xx => "XX"
    }
  }

  final case class SetOpts(
    setSeconds: Option[Long],
    setMilliseconds: Option[Long],
    setCondition: Option[Condition],
    keepTTL: Boolean
  )
  object SetOpts{
    val default = SetOpts(None, None, None, false)
  }

  def set[F[_]: RedisCtx](key: String, value: String, setOpts: SetOpts = SetOpts.default): F[Option[Status]] = {
    val ex = setOpts.setSeconds.toList.flatMap(l => List("EX", l.encode))
    val px = setOpts.setMilliseconds.toList.flatMap(l => List("PX", l.encode))
    val condition = setOpts.setCondition.toList.map(_.encode)
    val keepTTL = Alternative[List].guard(setOpts.keepTTL).as("KEEPTTL")
    RedisCtx[F].keyed(key, NEL("SET", key.encode :: value.encode :: ex ::: px ::: condition ::: keepTTL))
  }



  def setBV[F[_]: RedisCtx](key: ByteVector, value: ByteVector, setOpts: SetOpts = SetOpts.default): F[Option[Status]] = {
    val ex = setOpts.setSeconds.toList.flatMap(l => List("EX", l.encode)).map(toBV)
    val px = setOpts.setMilliseconds.toList.flatMap(l => List("PX", l.encode)).map(toBV)
    val condition = setOpts.setCondition.toList.map(_.encode).map(toBV)
    val keepTTL = Alternative[List].guard(setOpts.keepTTL).as("KEEPTTL").map(toBV)
    RedisCtx[F].keyedBV(key, NEL(toBV("SET"), key :: value :: ex ::: px ::: condition ::: keepTTL))
  }

  final case class ZAddOpts(
    condition: Option[Condition],
    change: Boolean,
    increment: Boolean
  )
  object ZAddOpts {
    val default = ZAddOpts(None, false, false)
  }

  def zadd[F[_]: RedisCtx](key: String, scoreMember: List[(Double, String)], options: ZAddOpts = ZAddOpts.default): F[Long] = {
    val scores = scoreMember.flatMap{ case (x, y) => List(x.encode, y.encode)}
    val condition = options.condition.toList.map(_.encode)
    val change = Alternative[List].guard(options.change).as("CH")
    val increment = Alternative[List].guard(options.increment).as("INCR")
    RedisCtx[F].keyed(key, NEL("ZADD", key :: condition ::: change ::: increment ::: scores))
  }

  sealed trait ReplyMode
  object ReplyMode {
    case object On extends ReplyMode
    case object Off extends ReplyMode
    case object Skip extends ReplyMode

    implicit val arg: RedisArg[ReplyMode] = RedisArg[String].contramap[ReplyMode]{
      case On => "ON"
      case Off => "OFF"
      case Skip => "SKIP"
    }
  }

  def clientreply[F[_]: RedisCtx](mode: ReplyMode): F[Boolean] = 
    RedisCtx[F].unkeyed(NEL.of("CLIENT REPLY", mode.encode))

  def srandmember[F[_]: RedisCtx](key: String): F[Option[String]] = 
    RedisCtx[F].keyed(key, NEL.of("SRANDMEMBER", key.encode))

  def srandmemberMulti[F[_]: RedisCtx](key: String, count: Long): F[List[String]] = 
    RedisCtx[F].keyed(key, NEL.of("SRANDMEMBER", key.encode, count.encode))

  def spop[F[_]: RedisCtx](key: String): F[Option[String]] = 
    RedisCtx[F].keyed(key, NEL.of("SPOP", key.encode))

  def spopMulti[F[_]: RedisCtx](key: String,  count: Long): F[List[String]] = 
    RedisCtx[F].keyed(key, NEL.of("SPOP", key.encode, count.encode))

  def info[F[_]: RedisCtx]: F[String] = 
    RedisCtx[F].unkeyed(NEL.of("INFO"))

  def infosection[F[_]: RedisCtx](section: String): F[String] = 
    RedisCtx[F].unkeyed(NEL.of("INFO", section.encode))

  def exists[F[_]: RedisCtx](key: String): F[Boolean] = 
    RedisCtx[F].keyed(key, NEL.of("EXISTS", key.encode))
  
  // TODO Scan
  // TODO LEX

  sealed trait Trimming
  object Trimming {
    case object Approximate extends Trimming
    case object Exact extends Trimming
    implicit val arg: RedisArg[Trimming] = RedisArg[String].contramap[Trimming]{
      case Approximate => "~"
      case Exact => "="
    }
  }

  final case class XAddOpts(
    id: Option[String],
    maxLength: Option[Long],
    trimming: Option[Trimming],
    noMkStream: Boolean,
    minId: Option[String],
    limit: Option[Long]
  )
  object XAddOpts {
    val default = XAddOpts(None, None, None, false, None, None)
  }

  def xadd[F[_]: RedisCtx](stream: String, body: List[(String, String)], xaddOpts: XAddOpts = XAddOpts.default): F[String] = {
    val maxLen = xaddOpts.maxLength.toList.flatMap{ l => List("MAXLEN".some, xaddOpts.trimming.map(_.encode), l.encode.some).flattenOption }
    val minId = xaddOpts.minId.toList.flatMap{ l => List("MINID".some, xaddOpts.trimming.map(_.encode), l.encode.some).flattenOption }
    val limit = xaddOpts.limit.toList.flatMap(l=> if (xaddOpts.trimming.contains(Trimming.Approximate)) List("LIMIT", l.encode) else List.empty)
    val noMkStream = Alternative[List].guard(xaddOpts.noMkStream).as("NOMKSTREAM")
    val id = List(xaddOpts.id.getOrElse("*"))
    val bodyEnd = body.foldLeft(List.empty[String]){ case (s, (k,v)) => s ::: List(k.encode, v.encode) }

    RedisCtx[F].unkeyed(NEL("XADD", stream :: maxLen ::: minId ::: limit ::: noMkStream ::: id ::: bodyEnd))
  }

  final case class XReadOpts(
    blockMillisecond: Option[Long],
    count: Option[Long],
    noAck: Boolean
  )
  object XReadOpts {
    val default = XReadOpts(None, None, false)
  }

  sealed trait StreamOffset {
    def stream: String
    def offset: String
  }

  object StreamOffset {
    case class All(stream: String) extends StreamOffset {
      override def offset: String = "0"
    }
    case class Latest(stream: String) extends StreamOffset {
      override def offset: String = "$"
    }
    /** Only applicable when using with xreadgroup */
    case class LastConsumed(stream: String) extends StreamOffset {
      override def offset: String = ">"
    }
    case class From(stream: String, offset: String) extends StreamOffset 
  }

  final case class StreamsRecord(
    recordId: String,
    keyValues: List[(String, String)]
  )

  object StreamsRecord {
    implicit val result : RedisResult[StreamsRecord] = new RedisResult[StreamsRecord] {
      def decode(resp: Resp): Either[Resp,StreamsRecord] = {
        def two[A](l: List[A], acc: List[(A, A)] = List.empty): List[(A, A)] = l match {
          case first :: second :: rest => two(rest, (first, second):: acc)
          case _ => acc.reverse
        }
        resp match {
          case  Resp.Array(Some(Resp.BulkString(Some(recordIdBV)) :: Resp.Array(Some(rawKeyValues)) :: Nil)) => 
            for {
              recordId <- recordIdBV.decodeUtf8.leftMap(_ => resp)
              keyValuesList <- rawKeyValues.traverse(RedisResult[String].decode).map(two(_))
            } yield StreamsRecord(recordId, keyValuesList)
          case otherwise => Left(otherwise)
        }
      }
    }
  }

  final case class XReadResponse(
    stream: String,
    records: List[StreamsRecord]
  )
  object XReadResponse{
    implicit val result: RedisResult[XReadResponse] = new RedisResult[XReadResponse] {
      def decode(resp: Resp): Either[Resp,XReadResponse] = {
        resp match {
          case Resp.Array(Some(Resp.BulkString(Some(streamBV)) :: Resp.Array(Some(list)) :: Nil)) => 
            streamBV.decodeUtf8.leftMap(_ => resp).flatMap{ stream => 
              list.traverse(RedisResult[StreamsRecord].decode).map(l => 
                XReadResponse(stream, l)
              )
            }
          case otherwise => Left(otherwise)
        }
      }
    }
  }

  def xread[F[_]: RedisCtx](streams: Set[StreamOffset], xreadOpts: XReadOpts = XReadOpts.default): F[Option[List[XReadResponse]]] = {//F[Option[List[List[(String, List[List[(String, List[(String, String)])]])]]]] = {
    val block = xreadOpts.blockMillisecond.toList.flatMap(l => List("BLOCK", l.encode))
    val count = xreadOpts.count.toList.flatMap(l => List("COUNT", l.encode))
    val noAck = Alternative[List].guard(xreadOpts.noAck).as("NOACK")
    val (streamKeys, streamOffsets) = streams
      .foldLeft((List.empty[String], List.empty[String])){ 
        case ((keys, offsets), streamOffset) => 
          (streamOffset.stream :: keys, streamOffset.offset :: offsets)
      }
    val streamPairs = "STREAMS" :: streamKeys ::: streamOffsets

    RedisCtx[F].unkeyed(NEL("XREAD", block ::: count ::: noAck ::: streamPairs))
  }

  case class Consumer(group: String, name: String)

  def xreadgroup[F[_]: RedisCtx](consumer: Consumer, streams: Set[StreamOffset], xreadOpts: XReadOpts = XReadOpts.default): F[Option[List[XReadResponse]]] = {
    val group = List("GROUP", consumer.group, consumer.name)
    val block = xreadOpts.blockMillisecond.toList.flatMap(l => List("BLOCK", l.encode))
    val count = xreadOpts.count.toList.flatMap(l => List("COUNT", l.encode))
    val noAck = Alternative[List].guard(xreadOpts.noAck).as("NOACK")
    val (streamKeys, streamOffsets) = streams
      .foldLeft((List.empty[String], List.empty[String])){ 
        case ((keys, offsets), streamOffset) => 
          (streamOffset.stream :: keys, streamOffset.offset :: offsets)
      }
    val streamPairs = "STREAMS" :: streamKeys ::: streamOffsets

    RedisCtx[F].unkeyed(NEL("XREADGROUP", group ::: block ::: count ::: noAck ::: streamPairs))
  }

  def xrange[F[_]: RedisCtx](stream: String, startOpt: Option[String] = None, endOpt: Option[String] = None, countOpt: Option[Int] = None): F[Option[List[StreamsRecord]]] = {
    val start = List(startOpt.getOrElse("-"))
    val end = List(endOpt.getOrElse("+"))
    val count = countOpt.toList.flatMap(l => List("COUNT", l.encode))

    RedisCtx[F].unkeyed(NEL("XRANGE", stream :: start ::: end ::: count))
  }

  def xrevrange[F[_]: RedisCtx](stream: String, endOpt: Option[String] = None, startOpt: Option[String] = None, countOpt: Option[Int] = None): F[Option[List[StreamsRecord]]] = {
    val end = List(endOpt.getOrElse("+"))
    val start = List(startOpt.getOrElse("-"))
    val count = countOpt.toList.flatMap(l => List("COUNT", l.encode))

    RedisCtx[F].unkeyed(NEL("XREVRANGE", stream :: end ::: start ::: count))
  }

  def xgroupcreate[F[_]: RedisCtx](stream: String, groupName: String, startId: String, mkStream: Boolean = false): F[Status] = {
    val mkStreamFragment = Alternative[List].guard(mkStream).as("MKSTREAM")
    RedisCtx[F].unkeyed(NEL("XGROUP", "CREATE" :: stream :: groupName :: startId :: mkStreamFragment))
  }

  @deprecated("use xgroupcreate(stream: String, groupName: String, startId: String, mkStream: Boolean) instead", "0.3.3")
  def xgroupcreate[F[_]](stream: String, groupName: String, startId: String, ctx: RedisCtx[F]): F[Status] = 
    xgroupcreate(stream, groupName, startId, false)(ctx)

  def xgroupsetid[F[_]: RedisCtx](stream: String, groupName: String, messageId: String): F[Status] = 
    RedisCtx[F].unkeyed(NEL.of("XGROUP", "SETID", stream, groupName, messageId))

  def xgroupdelconsumer[F[_]: RedisCtx](stream: String, groupName: String, consumer: String): F[Long] = 
    RedisCtx[F].unkeyed(NEL.of("XGROUP", "DELCONSUMER", stream, groupName, consumer))

  def xgroupdestroy[F[_]: RedisCtx](stream: String, groupName: String): F[Boolean] =
    RedisCtx[F].unkeyed(NEL.of("XGROUP", "DESTROY", stream, groupName))

  def xack[F[_]: RedisCtx](stream: String, groupName: String, messageIds: List[String]): F[Long] = 
    RedisCtx[F].unkeyed(NEL("XACK", stream :: groupName :: messageIds))

  def xlen[F[_]: RedisCtx](stream: String): F[Long] = 
    RedisCtx[F].unkeyed(NEL.of("XLEN", stream))
  
  sealed trait XTrimStrategy
  object XTrimStrategy {
    case class MaxLen(maxLen: Int) extends XTrimStrategy
    case class MinId(minId: String) extends XTrimStrategy
  }
  
  def xtrim[F[_]: RedisCtx](stream: String, strategy: XTrimStrategy, trimmingOpt: Option[Trimming] = None, limitOpt: Option[Int] = None): F[Int] = {
    val strategyFragment = strategy match {
      case XTrimStrategy.MaxLen(maxLen) => List("MAXLEN".some, trimmingOpt.map(_.encode), maxLen.encode.some).flatten
      case XTrimStrategy.MinId(minId) => List("MINID".some, trimmingOpt.map(_.encode), minId.encode.some).flatten
    }

    val limit = limitOpt.toList.flatMap(l => if (trimmingOpt.contains(Trimming.Approximate)) List("LIMIT", l.encode) else List.empty)

    RedisCtx[F].unkeyed(NEL("XTRIM", stream :: strategyFragment ::: limit))
  }

  final case class XPendingSummary(
    totalPending: Long,
    smallestId: String,
    greatestId: String,
    consumerPendings: List[(String, Int)]
  )
  object XPendingSummary {
    implicit val result: RedisResult[XPendingSummary] = new RedisResult[XPendingSummary] {
      def decode(resp: Resp): Either[Resp,XPendingSummary] = 
        resp match {
          case Resp.Array(Some(Resp.Integer(totalPending) :: Resp.BulkString(Some(smallestIdBV)) :: Resp.BulkString(Some(greatestIdBV)) :: Resp.Array(Some(list)) :: Nil)) => 
            for {
              smallestId <- smallestIdBV.decodeUtf8.leftMap(_ => resp)
              greatestId <- greatestIdBV.decodeUtf8.leftMap(_ => resp)
              consumerPendings <- list.traverse{ 
                                    case Resp.Array(Some(Resp.BulkString(Some(consumerNameBV)) :: Resp.BulkString(Some(pendingCountBV)) :: Nil)) =>  
                                      (consumerNameBV.decodeUtf8, pendingCountBV.decodeUtf8.map(_.toInt))
                                        .tupled
                                        .leftMap(_ => resp)
                                    case _ => Left(resp)
                                  }
            } yield XPendingSummary(totalPending, smallestId, greatestId, consumerPendings)
            
          case otherwise => Left(otherwise)
        }
    }
  }

  def xpendingsummary[F[_]: RedisCtx](stream: String, groupName: String): F[XPendingSummary] = 
    RedisCtx[F].unkeyed(NEL.of("XPENDING", stream, groupName))

  // TOOD xpendingdetail
  // TODO xinfo

  final case class XClaimArgs(
    minIdleTime: Long,
    idle: Option[Long] = None,
    time: Option[Long] = None,
    retrycount: Option[Long] = None,
    force: Boolean = false,
  )

  private def xclaimraw[F[_]: RedisCtx, A: RedisResult](stream: String, consumer: Consumer, args: XClaimArgs, justId: Boolean, messageIds: List[String]): F[A] = {
    val consumerFragment = List(consumer.group, consumer.name)
    val minIdleTime = List(args.minIdleTime.encode)
    val idle = args.idle.toList.flatMap(l => List("IDLE", l.encode))
    val time = args.time.toList.flatMap(l => List("TIME", l.encode))
    val retrycount = args.retrycount.toList.flatMap(l => List("RETRYCOUNT", l.encode))
    val force = Alternative[List].guard(args.force).as("FORCE")
    val justIdFragment = Alternative[List].guard(justId).as("JUSTID")
    val argFragment = idle ::: time ::: retrycount ::: force ::: justIdFragment
    RedisCtx[F].unkeyed(NEL("XCLAIM", stream :: consumerFragment ::: minIdleTime ::: messageIds ::: argFragment))
  }

  def xclaimsummary[F[_]: RedisCtx](stream: String, consumer: Consumer, args: XClaimArgs, messageIds: List[String]): F[List[String]] = 
    xclaimraw(stream, consumer, args, true, messageIds)

  def xclaimdetail[F[_]: RedisCtx](stream: String, consumer: Consumer, args: XClaimArgs, messageIds: List[String]): F[List[StreamsRecord]] = 
    xclaimraw(stream, consumer, args, false, messageIds)

  final case class XAutoClaimArgs(
    consumer: Consumer,
    minIdleTime: Long,
    startId: String,
    count: Option[Long] = None,
  )

  final case class XAutoClaimSummary(
    startId: String,
    claimedMsgIds: List[String],
    deletedIds: List[String]
  )
  object XAutoClaimSummary {
    implicit val result: RedisResult[XAutoClaimSummary] = new RedisResult[XAutoClaimSummary] {
      def decode(resp: Resp): Either[Resp,XAutoClaimSummary] = 
        resp match {
          case Resp.Array(Some(startId :: claimedMsgIds :: deletedIds :: Nil)) => 
            (RedisResult[String].decode(startId), RedisResult[List[String]].decode(claimedMsgIds), RedisResult[List[String]].decode(deletedIds))
              .tupled
              .map((XAutoClaimSummary.apply _).tupled)
          case otherwise => Left(otherwise)
        }
    }
  }
  final case class XAutoClaimDetail(
    startId: String,
    claimedMsgIds: List[StreamsRecord],
    deletedIds: List[String]
  )
  object XAutoClaimDetail {
    implicit val result: RedisResult[XAutoClaimDetail] = new RedisResult[XAutoClaimDetail] {
      def decode(resp: Resp): Either[Resp,XAutoClaimDetail] = 
        resp match {
          case Resp.Array(Some(startId :: claimedMsgs :: deletedIds :: Nil)) => 
            (RedisResult[String].decode(startId), RedisResult[List[StreamsRecord]].decode(claimedMsgs), RedisResult[List[String]].decode(deletedIds))
              .tupled
              .map((XAutoClaimDetail.apply _).tupled)
          case otherwise => Left(otherwise)
        }
    }
  }
  
  private def xautoclaimraw[F[_]: RedisCtx, A: RedisResult](stream: String, args: XAutoClaimArgs, justId: Boolean): F[A] = {
    val consumer = List(args.consumer.group, args.consumer.name)
    val minIdleTime = List(args.minIdleTime.encode)
    val startId = List(args.startId)
    val count = args.count.toList.flatMap(l => List("COUNT", l.encode))
    val justIdFragment = Alternative[List].guard(justId).as("JUSTID")
    val argFragment = consumer ::: minIdleTime ::: startId ::: count ::: justIdFragment 
    RedisCtx[F].unkeyed(NEL("XAUTOCLAIM", stream :: argFragment))
  }

  def xautoclaimsummary[F[_]: RedisCtx](stream: String, args: XAutoClaimArgs): F[XAutoClaimSummary] = 
    xautoclaimraw(stream, args, true)

  def xautoclaimdetail[F[_]: RedisCtx](stream: String, args: XAutoClaimArgs): F[XAutoClaimDetail] = 
    xautoclaimraw(stream, args, false)

  def xdel[F[_]: RedisCtx](stream: String, messageIds: List[String]): F[Long] = 
    RedisCtx[F].unkeyed(NEL("XDEL", stream :: messageIds))

  // Simple String Commands

  def ping[F[_]: RedisCtx]: F[Status] =
    RedisCtx[F].unkeyed[Status](NEL.of("PING"))

  def ttl[F[_]: RedisCtx](key: String): F[Long] = 
    RedisCtx[F].keyed(key, NEL.of("TTL", key.encode))

  def setnx[F[_]: RedisCtx](key: String, value: String): F[Boolean] = 
    RedisCtx[F].keyed(key, NEL.of("SETNX", key.encode, value.encode))

  def pttl[F[_]: RedisCtx](key: String): F[Long] =
    RedisCtx[F].keyed(key, NEL.of("PTTL", key.encode))

  def commandcount[F[_]: RedisCtx]: F[Long] = 
    RedisCtx[F].unkeyed(NEL.of("COMMAND", "COUNT"))

  def clientsetname[F[_]: RedisCtx](connectionName: String): F[String] = 
    RedisCtx[F].unkeyed(NEL.of("CLIENT", "SETNAME", connectionName.encode))

  def zrank[F[_]: RedisCtx](key: String, member: String): F[Long] = 
    RedisCtx[F].keyed(key, NEL.of("ZRANK", key.encode, member.encode))

  def zremrangebyscore[F[_]: RedisCtx](key: String, min: Double, max: Double): F[Long] = 
    RedisCtx[F].keyed(key, NEL.of("ZREMRANGEBYSCORE", key.encode, min.encode, max.encode))

  def hkeys[F[_]: RedisCtx](key: String): F[List[String]] = 
    RedisCtx[F].keyed(key, NEL.of("HKEYS", key.encode))

  def slaveof[F[_]: RedisCtx](host: String, port: Int): F[Status] = 
    RedisCtx[F].unkeyed(NEL.of("SLAVEOF", host.encode, port.encode))

  def rpushx[F[_]: RedisCtx](key: String, value: String): F[Long] = 
    RedisCtx[F].keyed(key, NEL.of("RPUSHX", key.encode, value.encode))

  def debugobject[F[_]: RedisCtx](key: String): F[String] = 
    RedisCtx[F].keyed(key, NEL.of("DEBUG", "OBJECT", key.encode))

  def bgsave[F[_]: RedisCtx]: F[Status] = 
    RedisCtx[F].unkeyed(NEL.of("BGSAVE"))

  def hlen[F[_]: RedisCtx](key: String): F[Long] = 
    RedisCtx[F].keyed(key, NEL.of("HLEN", key.encode))

  def rpoplpush[F[_]: RedisCtx](source: String, destination: String): F[Option[String]] = 
    RedisCtx[F].keyed(source, NEL.of("RPOPLPUSH", source.encode, destination.encode))

  def brpop[F[_]: RedisCtx](key: List[String], timeout: Long): F[Option[(String, String)]] = {
    val cmd =  NEL.of("BRPOP", (key.map(_.encode) ++ List(timeout.encode)):_*)
    key match {
      case Nil => RedisCtx[F].unkeyed(cmd)
      case k :: _ => RedisCtx[F].keyed(k, cmd)
    }
  }

  def bgrewriteaof[F[_]: RedisCtx]: F[Status] = 
    RedisCtx[F].unkeyed(NEL.of("BGREWRITEAOF"))

  def zincrby[F[_]: RedisCtx](key: String, increment: Long, member: String): F[Double] = 
    RedisCtx[F].keyed(key, NEL.of("ZINCRBY", key.encode, increment.encode, member.encode))

  def hgetall[F[_]: RedisCtx](key: String): F[List[(String, String)]] = 
    RedisCtx[F].keyed(key, NEL.of("HGETALL", key.encode))

  def hmset[F[_]: RedisCtx](key: String, fieldValue: List[(String, String)]): F[Status] = 
    RedisCtx[F].keyed(key, NEL("HMSET", key.encode :: fieldValue.flatMap{ case (x, y) => List(x.encode, y.encode)}))

  def sinter[F[_]: RedisCtx](key: List[String]): F[List[String]] = {
    val cmd = NEL("SINTER", key.map(_.encode))
    key match {
      case Nil => RedisCtx[F].unkeyed(cmd)
      case k :: _ => RedisCtx[F].keyed(k, cmd)
    }
  }

  def pfadd[F[_]: RedisCtx](key: String, value: List[String]): F[Long] = 
    RedisCtx[F].keyed(key, NEL("PFADD", key.encode :: value.map(_.encode)))

  def zremrangebyrank[F[_]: RedisCtx](key: String, start: Long, stop: Long): F[Long] = 
    RedisCtx[F].keyed(key, NEL.of("ZREMRANGEBYRANK", key.encode, start.encode, stop.encode))

  def flushdb[F[_]: RedisCtx]: F[Status] = 
    RedisCtx[F].unkeyed(NEL.of("FLUSHDB"))

  def sadd[F[_]: RedisCtx](key: String, member: List[String]): F[Long] = 
    RedisCtx[F].keyed(key, NEL("SADD", key.encode :: member.map(_.encode)))

  def lindex[F[_]: RedisCtx](key: String, index: Int): F[Option[String]] = 
    RedisCtx[F].keyed(key, NEL.of("LINDEX", key.encode, index.encode))

  def lpush[F[_]: RedisCtx](key: String, value: List[String]): F[Long] = 
    RedisCtx[F].keyed(key, NEL("LPUSH", key.encode :: value.map(_.encode)))

  def hstrlen[F[_]: RedisCtx](key: String, field: String): F[Long] = 
    RedisCtx[F].keyed(key, NEL.of("HSTRLEN", key.encode, field.encode))

  def smove[F[_]: RedisCtx](source: String, destination: String, member: String): F[Boolean] = 
    RedisCtx[F].keyed(source, NEL.of("SMOVE", source.encode, destination.encode, member.encode))

  def zscore[F[_]: RedisCtx](key: String, member: String): F[Option[Double]] = 
    RedisCtx[F].keyed(key, NEL.of("ZSCORE", key.encode, member.encode))

  def configresetstat[F[_]: RedisCtx]: F[Status] = 
    RedisCtx[F].unkeyed(NEL.of("CONFIG", "RESETSTAT"))

  def pfcount[F[_]: RedisCtx](key: List[String]): F[Long] = {
    val cmd = NEL("PFCOUNT", key.map(_.encode))
    key match {
      case Nil => RedisCtx[F].unkeyed(cmd)
      case k :: _ => RedisCtx[F].keyed(k, cmd)
    }
  }

  def hdel[F[_]: RedisCtx](key: String, field: List[String]): F[Long] = 
    RedisCtx[F].keyed(key, NEL("HDEL", key.encode :: field.map(_.encode)))

  def incrbyfloat[F[_]: RedisCtx](key: String, increment: Double): F[Double] = 
    RedisCtx[F].keyed(key, NEL.of("INCRBYFLOAT", key.encode, increment.encode))

  def setbit[F[_]: RedisCtx](key: String, offset: Long, value: String): F[Long] =
    RedisCtx[F].keyed(key, NEL.of("SETBIT", key.encode, offset.encode, value.encode))

  def flushall[F[_]: RedisCtx]: F[Status] = 
    RedisCtx[F].unkeyed(NEL.of("FLUSHALL"))

  def incrby[F[_]: RedisCtx](key: String, increment: Long): F[Long] = 
    RedisCtx[F].keyed(key, NEL.of("INCRBY", key.encode, increment.encode))

  def time[F[_]: RedisCtx]: F[(Long, Long)] = 
    RedisCtx[F].unkeyed(NEL.of("TIME"))

  def smembers[F[_]: RedisCtx](key: String): F[List[String]] = 
    RedisCtx[F].keyed(key, NEL.of("SMEMBERS", key.encode))

  def zlexcount[F[_]: RedisCtx](key: String, min: String, max: String): F[Long] = 
    RedisCtx[F].keyed(key, NEL.of("ZLEXCOUNT", key.encode, min.encode, max.encode))

  def sunion[F[_]: RedisCtx](key: List[String]): F[List[String]] = {
    val cmd = NEL("SUNION", key.map(_.encode))
    key match {
      case Nil => RedisCtx[F].unkeyed(cmd)
      case k :: _ => RedisCtx[F].keyed(k, cmd)
    }
  }

  def sinterstore[F[_]: RedisCtx](destination: String, key: List[String]): F[Long] = 
    RedisCtx[F].keyed(destination, NEL("SINTERSTORE", destination.encode :: key.map(_.encode)))

  def hvals[F[_]: RedisCtx](key: String): F[List[String]] = 
    RedisCtx[F].keyed(key, NEL.of("HVALS", key.encode))

  def configset[F[_]: RedisCtx](parameter: String, value: String): F[Status] = 
    RedisCtx[F].unkeyed(NEL.of("CONFIG", "SET", parameter.encode, value.encode))

  def scriptflush[F[_]: RedisCtx]: F[Status] = 
    RedisCtx[F].unkeyed(NEL.of("SCRIPT", "FLUSH"))

  def dbsize[F[_]: RedisCtx]: F[Long] = 
    RedisCtx[F].unkeyed(NEL.of("DBSIZE"))

  def wait[F[_]: RedisCtx](numslaves: Long, timeout: Long): F[Long] = 
    RedisCtx[F].unkeyed(NEL.of("WAIT", numslaves.encode, timeout.encode))

  def lpop[F[_]: RedisCtx](key: String): F[Option[String]] = 
    RedisCtx[F].keyed(key, NEL.of("LPOP", key.encode))

  def clientpause[F[_]: RedisCtx](timeout: Long): F[Status] = 
    RedisCtx[F].unkeyed(NEL.of("CLIENT", "PAUSE", timeout.encode))

  def expire[F[_]: RedisCtx](key: String, seconds: Long): F[Boolean] = 
    RedisCtx[F].keyed(key, NEL.of("EXPIRE", key.encode, seconds.encode))

  def mget[F[_]: RedisCtx](key: String): F[List[Option[String]]] = {
    val cmd = NEL("MGET", key.encode :: Nil)
    RedisCtx[F].keyed(key, cmd)
  }

  def bitpos[F[_]: RedisCtx](key: String, bit: Long, start: Long, end: Long): F[Long] = 
    RedisCtx[F].keyed(key, NEL.of("BITPOS", key.encode, bit.encode, start.encode, end.encode))

  def lastsave[F[_]: RedisCtx]: F[Long] = 
    RedisCtx[F].unkeyed(NEL.of("LASTSAVE"))

  def pexpire[F[_]: RedisCtx](key: String, milliseconds: Long): F[Boolean] = 
    RedisCtx[F].keyed(key, NEL.of("PEXPIRE", key.encode, milliseconds.encode))

  def clientlist[F[_]: RedisCtx]: F[List[String]] = 
    RedisCtx[F].unkeyed(NEL.of("CLIENT", "LIST"))

  def renamenx[F[_]: RedisCtx](key: String, newkey: String): F[Boolean] = 
    RedisCtx[F].keyed(key, NEL.of("RENAMENX", key.encode, newkey.encode))

  def pfmerge[F[_]: RedisCtx](destkey: String, sourcekey: List[String]): F[String] = 
    RedisCtx[F].keyed(destkey, NEL("PFMERGE", destkey.encode :: sourcekey.map(_.encode)))

  def lrem[F[_]: RedisCtx](key: String, count: Long, value: String): F[Long] = 
    RedisCtx[F].keyed(key, NEL.of("LREM", key.encode, count.encode, value.encode))

  def sdiff[F[_]: RedisCtx](key: List[String]): F[List[String]] = {
    val cmd = NEL("SDIFF", key.map(_.encode))
    key match {
      case Nil => RedisCtx[F].unkeyed(cmd)
      case key :: _ => RedisCtx[F].keyed(key, cmd)
    }
  }

  def get[F[_]: RedisCtx](key: String): F[Option[String]] = 
    RedisCtx[F].keyed(key, NEL.of("GET", key.encode))

  def getBV[F[_]: RedisCtx](key: ByteVector): F[Option[ByteVector]] = 
    RedisCtx[F].keyedBV(key, NEL.of(toBV("GET"), key))

  def getrange[F[_]: RedisCtx](key: String, start: Long, end: Long): F[String] = 
    RedisCtx[F].keyed(key, NEL.of("GETRANGE", key.encode, start.encode, end.encode))

  def sdiffstore[F[_]: RedisCtx](destination: String, key: List[String]): F[Long] = 
    RedisCtx[F].keyed(destination, NEL("SDIFFSTORE", destination.encode :: key.map(_.encode)))

  def zcount[F[_]: RedisCtx](key: String, min: Double, max: Double): F[Long] =
    RedisCtx[F].keyed(key, NEL.of("ZCOUNT", key.encode, min.encode, max.encode))

  def scriptload[F[_]: RedisCtx](script: String): F[String] =
    RedisCtx[F].unkeyed(NEL.of("SCRIPT", "LOAD", script.encode))

  def getset[F[_]: RedisCtx](key: String, value: String): F[Option[String]] = 
    RedisCtx[F].keyed(key, NEL.of("GETSET", key.encode, value.encode))

  def dump[F[_]: RedisCtx](key: String): F[String] = 
    RedisCtx[F].keyed(key, NEL.of("DUMP", key.encode))

  def keys[F[_]: RedisCtx](pattern: String): F[List[String]] = 
    RedisCtx[F].unkeyed(NEL.of("KEYS", pattern.encode))

  def configget[F[_]: RedisCtx](parameter: String): F[List[(String, String)]] = 
    RedisCtx[F].unkeyed(NEL.of("CONFIG", "GET", parameter.encode))

  def rpush[F[_]: RedisCtx](key: String, value: List[String]): F[Long] = 
    RedisCtx[F].keyed(key, NEL("RPUSH", key.encode :: value.map(_.encode)))

  def randomkey[F[_]: RedisCtx]: F[Option[String]] = 
    RedisCtx[F].unkeyed(NEL.of("RANDOMKEY"))

  def hsetnx[F[_]: RedisCtx](key: String, field: String, value: String): F[Boolean] = 
    RedisCtx[F].keyed(key, NEL.of("HSETNX", key.encode, field.encode, value.encode))

  def mset[F[_]: RedisCtx](keyvalue: (String, String)): F[Status] = 
    RedisCtx[F].keyed(keyvalue._1, NEL("MSET", List(keyvalue._1.encode, keyvalue._2.encode)))

  def setex[F[_]: RedisCtx](key: String, seconds: Long, value: String): F[Status] = 
    RedisCtx[F].keyed(key, NEL.of("SETEX", key.encode, seconds.encode, value.encode))

  def psetex[F[_]: RedisCtx](key: String, milliseconds: Long, value: String): F[Status] = 
    RedisCtx[F].keyed(key, NEL.of("PSETEX", key.encode, milliseconds.encode, value.encode))

  def scard[F[_]: RedisCtx](key: String): F[Long] = 
    RedisCtx[F].keyed(key, NEL.of("SCARD", key.encode))

  def scriptexists[F[_]: RedisCtx](script: List[String]): F[List[Boolean]] = 
    RedisCtx[F].unkeyed(NEL("SCRIPT", "EXISTS" :: script.map(_.encode)))

  def sunionstore[F[_]: RedisCtx](destination: String, key: List[String] ): F[Long] = 
    RedisCtx[F].keyed(destination, NEL("SUNIONSTORE", destination.encode :: key.map(_.encode)))

  def persist[F[_]: RedisCtx](key: String): F[Boolean] = 
    RedisCtx[F].keyed(key, NEL.of("PERSIST", key.encode))

  def strlen[F[_]: RedisCtx](key: String): F[Long] = 
    RedisCtx[F].keyed(key, NEL.of("STRLEN", key.encode))

  def lpushx[F[_]: RedisCtx](key: String, value: String): F[Long] = 
    RedisCtx[F].keyed(key, NEL.of("LPUSHX", key.encode, value.encode))

  def hset[F[_]: RedisCtx](key: String, field: String, value: String): F[Boolean] = 
    RedisCtx[F].keyed(key, NEL.of("HSET", key.encode, field.encode, value.encode))

  def brpoplpush[F[_]: RedisCtx](source: String, destination: String, timeout: Long): F[Option[String]] = 
    RedisCtx[F].keyed(source, NEL.of("BRPOPLPUSH", source.encode, destination.encode, timeout.encode))

  def zrevrank[F[_]: RedisCtx](key: String, member: String): F[Option[Long]] = 
    RedisCtx[F].keyed(key, NEL.of("ZREVRANK", key.encode, member.encode))

  def scriptkill[F[_]: RedisCtx]: F[Status] = 
    RedisCtx[F].unkeyed(NEL.of("SCRIPT", "KILL"))

  def setrange[F[_]: RedisCtx](key: String, offset: Long, value: String): F[Long] = 
    RedisCtx[F].keyed(key, NEL.of("SETRANGE", key.encode, offset.encode, value.encode))

  def del[F[_]: RedisCtx](key: String): F[Long] = 
    RedisCtx[F].keyed(key, NEL("DEL", key.encode :: Nil))

  def hincrbyfloat[F[_]: RedisCtx](key: String,field: String,increment: Double): F[Double] = 
    RedisCtx[F].keyed(key, NEL.of("HINCRBYFLOAT", key.encode, field.encode, increment.encode))


  def hincrby[F[_]: RedisCtx](key: String, field: String, increment: Long): F[Long] = 
    RedisCtx[F].keyed(key, NEL.of("HINCRBY", key.encode, field.encode, increment.encode))

  def zremrangebylex[F[_]: RedisCtx](key: String, min: String, max: String): F[Long]  =
    RedisCtx[F].keyed(key, NEL.of("ZREMRANGEBYLEX", key.encode, min.encode, max.encode))

  def rpop[F[_]: RedisCtx](key: String): F[Option[String]] = 
    RedisCtx[F].keyed(key, NEL.of("RPOP", key.encode))

  def rename[F[_]: RedisCtx](key: String, newkey: String): F[Status] = 
    RedisCtx[F].keyed(key, NEL.of("RENAME", key.encode, newkey.encode))

  def zrem[F[_]: RedisCtx](key: String, member: List[String]): F[Long] = 
    RedisCtx[F].keyed(key, NEL("ZREM", key.encode :: member.map(_.encode)))

  def hexists[F[_]: RedisCtx](key: String, field: String): F[Boolean] =
    RedisCtx[F].keyed(key, NEL.of("HEXISTS", key.encode, field.encode))

  def clientgetname[F[_]: RedisCtx]: F[Status] = 
    RedisCtx[F].unkeyed(NEL.of("CLIENT", "GETNAME"))

  def configerewrite[F[_]: RedisCtx]: F[Status] = 
    RedisCtx[F].unkeyed(NEL.of("CONFIG", "REWRITE"))

  def decr[F[_]: RedisCtx](key: String): F[Long] =
    RedisCtx[F].keyed(key, NEL.of("DECR", key.encode))

  def hmget[F[_]: RedisCtx](key: String, field: List[String]): F[List[Option[String]]] = 
    RedisCtx[F].keyed(key, NEL("HMGET", key.encode :: field.map(_.encode)))

  def lrange[F[_]: RedisCtx](key: String, start: Long, stop: Long): F[List[String]] = 
    RedisCtx[F].keyed(key, NEL.of("LRANGE", key.encode, start.encode, stop.encode))

  def decrby[F[_]: RedisCtx](key: String, decrement: Long): F[Long] = 
    RedisCtx[F].keyed(key, NEL.of("DECRBY", key.encode, decrement.encode))

  def llen[F[_]: RedisCtx](key: String): F[Long] = 
    RedisCtx[F].keyed(key, NEL.of("LLEN", key.encode))

  def append[F[_]: RedisCtx](key: String, value: String): F[Long] = 
    RedisCtx[F].keyed(key, NEL.of("APPEND", key.encode, value.encode))

  def incr[F[_]: RedisCtx](key: String): F[Long] =
    RedisCtx[F].keyed(key, NEL.of("INCR", key.encode))

  def hget[F[_]: RedisCtx](key: String, field: String): F[Option[String]] = 
    RedisCtx[F].keyed(key, NEL.of("HGET", key.encode, field.encode))

  def pexpireat[F[_]: RedisCtx](key: String, milliseconds: Long): F[Boolean] = 
    RedisCtx[F].keyed(key, NEL.of("PEXPIREAT", key.encode, milliseconds.encode))

  def ltrim[F[_]: RedisCtx](key: String, start: Long, stop: Long): F[Status] = 
    RedisCtx[F].keyed(key, NEL.of("LTRIM", key.encode, start.encode, stop.encode))

  def zcard[F[_]: RedisCtx](key: String): F[Long] = 
    RedisCtx[F].keyed(key, NEL.of("ZCARD", key.encode))

  def lset[F[_]: RedisCtx](key: String, index: Long, value: String): F[Status] = 
    RedisCtx[F].keyed(key, NEL.of("LSET", key.encode, index.encode, value.encode))

  def expireat[F[_]: RedisCtx](key: String, timestamp: Long): F[Boolean] =
    RedisCtx[F].keyed(key, NEL.of("EXPIREAT", key.encode, timestamp.encode))

  def save[F[_]: RedisCtx]: F[Status] = 
    RedisCtx[F].unkeyed(NEL.of("SAVE"))

  def move[F[_]: RedisCtx](key: String, db: Long): F[Boolean] = 
    RedisCtx[F].keyed(key, NEL.of("MOVE", key.encode, db.encode))

  def getbit[F[_]: RedisCtx](key: String, offset: Long): F[Long] = 
    RedisCtx[F].keyed(key, NEL.of("GETBIT", key.encode, offset.encode))

  def msetnx[F[_]: RedisCtx](keyvalue: List[(String, String)]): F[Boolean] = {
    val cmd = NEL("MSETNX", keyvalue.flatMap{case (x, y) => List(x.encode, y.encode)})
    keyvalue match {
      case Nil => RedisCtx[F].unkeyed(cmd)
      case (key, _) :: _ => RedisCtx[F].keyed(key, cmd)
    }
  }

  def commandinfo[F[_]: RedisCtx](commandName: List[String]): F[List[String]] = 
    RedisCtx[F].unkeyed(NEL("COMMAND", "INFO" :: commandName.map(_.encode)))

  def quit[F[_]: RedisCtx]: F[Status] = 
    RedisCtx[F].unkeyed(NEL.of("QUIT"))

  def blpop[F[_]: RedisCtx](key: List[String], timeout: Long): F[Option[(String, String)]] = {
    val cmd = NEL("BLPOP", key.map(_.encode) ++ List(timeout.encode))
    key.headOption match {
      case Some(value) => RedisCtx[F].keyed(value, cmd)
      case None => RedisCtx[F].unkeyed(cmd)
    }
  }
  
  def srem[F[_]: RedisCtx](key: String, member: List[String]): F[Long] = 
    RedisCtx[F].keyed(key, NEL("SREM", key.encode :: member.map(_.encode)))

  def echo[F[_]: RedisCtx](message: String): F[String] = 
    RedisCtx[F].unkeyed(NEL.of("ECHO", message.encode))

  def sismember[F[_]: RedisCtx](key: String, member: String): F[Boolean] = 
    RedisCtx[F].keyed(key, NEL.of("SISMEMBER", key.encode, member.encode))

  def publish[F[_]: RedisCtx](channel: String, message: String): F[Int] = 
    RedisCtx[F].unkeyed[Int](cats.data.NonEmptyList.of("PUBLISH", channel, message))

  private def toBV(s: String): ByteVector = ByteVector.encodeUtf8(s).fold(throw _, identity(_))
}
