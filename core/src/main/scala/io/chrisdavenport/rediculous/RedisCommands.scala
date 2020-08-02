package io.chrisdavenport.rediculous

import cats._
import cats.implicits._
import cats.effect._
import cats.data.{NonEmptyList => NEL}
import RedisProtocol._
import RedisConnection.runRequestTotal
import _root_.io.chrisdavenport.rediculous.implicits._

object RedisCommands {

  def objectrefcount[F[_]: Concurrent](key: String): Redis[F, Long] = 
    runRequestTotal(NEL.of("OBJECT", "refcount", key.encode))

  def objectidletime[F[_]: Concurrent](key: String): Redis[F, Long] = 
    runRequestTotal(NEL.of("OBJECT", "idletime", key.encode))

  def objectencoding[F[_]: Concurrent](key: String): Redis[F, String] =
    runRequestTotal(NEL.of("OBJECT", "encoding", key.encode))

  def linsertbefore[F[_]: Concurrent](key: String, pivot: String, value: String): Redis[F, Long] = 
    runRequestTotal(NEL.of("LINSERT", key.encode, "BEFORE", pivot.encode, value.encode))

  def lisertafter[F[_]: Concurrent](key: String, pivot: String, value: String): Redis[F, Long] = 
    runRequestTotal(NEL.of("LINSERT", key.encode, "AFTER", pivot.encode, value.encode))

  def getType[F[_]: Concurrent](key: String): Redis[F, RedisType] = 
    runRequestTotal(NEL.of("TYPE", key.encode))

  // TODO slow log commands

  def zrange[F[_]: Concurrent](key: String, start: Long, stop: Long): Redis[F, List[String]] = 
    runRequestTotal(NEL.of("ZRANGE", key.encode, start.encode, stop.encode))

  def zrangewithscores[F[_]: Concurrent](key: String, start: Long, stop: Long): Redis[F, List[(String, Double)]] = 
    runRequestTotal(NEL.of("ZRANGE", key.encode, start.encode, stop.encode, "WITHSCORES"))

  def zrevrange[F[_]: Concurrent](key: String, start: Long, stop: Long): Redis[F, List[String]] = 
    runRequestTotal(NEL.of("ZREVRANGE", key.encode, start.encode, stop.encode))

  def zrevrangewithscores[F[_]: Concurrent](key: String, start: Long, stop: Long): Redis[F, List[(String, Double)]] = 
    runRequestTotal(NEL.of("ZREVRANGE", key.encode, start.encode, stop.encode, "WITHSCORES"))

  def zrangebyscore[F[_]: Concurrent](key: String, min: Double, max: Double): Redis[F, List[String]] = 
    runRequestTotal(NEL.of("ZRANGEBYSCORE", key.encode, min.encode, max.encode))

  def zrangebyscorewithscores[F[_]: Concurrent](key: String, min: Double, max: Double): Redis[F, List[(String, Double)]] = 
    runRequestTotal(NEL.of("ZRANGEBYSCORE", key.encode, min.encode, max.encode, "WITHSCORES"))

  def zrangebyscorelimit[F[_]: Concurrent](key: String, min: Double, max: Double, offset: Long, count: Long): Redis[F, List[String]] =
    runRequestTotal(NEL.of("ZRANGEBYSCORE", key.encode, min.encode, max.encode, "LIMIT", offset.encode, count.encode))

  def zrangebyscorelimitwithscores[F[_]: Concurrent](key: String, min: Double, max: Double, offset: Long, count: Long): Redis[F, List[(String, Double)]] =
    runRequestTotal(NEL.of("ZRANGEBYSCORE", key.encode, min.encode, max.encode, "WITHSCORES", "LIMIT", offset.encode, count.encode))

  def zrevrangebyscore[F[_]: Concurrent](key: String, min: Double, max: Double): Redis[F, List[String]] = 
    runRequestTotal(NEL.of("ZREVRANGEBYSCORE", key.encode, min.encode, max.encode))

  def zrevrangebyscorewithscores[F[_]: Concurrent](key: String, min: Double, max: Double): Redis[F, List[(String, Double)]] =
    runRequestTotal(NEL.of("ZREVRANGEBYSCORE", key.encode, min.encode, max.encode, "WITHSCORES"))

  def zrevrangebyscorelimit[F[_]: Concurrent](key: String, min: Double, max: Double, offset: Long, count: Long): Redis[F, List[String]] = 
    runRequestTotal(NEL.of("ZREVRANGEBYSCORE", key.encode, min.encode, max.encode, "LIMIT", offset.encode, count.encode))

  def zrevrangebyscorelimitwithscores[F[_]: Concurrent](key: String, min: Double, max: Double, offset: Long, count: Long): Redis[F, List[(String, Double)]] = 
    runRequestTotal(NEL.of("ZREVRANGEBYSCORE", key.encode, min.encode, max.encode, "WITHSCORES", "LIMIT", offset.encode, count.encode))

  // TODO Sort
  // TODO aggregate

  def eval[F[_]: Concurrent, A: RedisResult](script: String, keys: List[String], args: List[String]): Redis[F, A] = 
    runRequestTotal(NEL("EVAL", script :: keys.length.encode :: keys ::: args))

  def evalsha[F[_]: Concurrent, A: RedisResult](script: String, keys: List[String], args: List[String]): Redis[F, A] = 
    runRequestTotal(NEL("EVALSHA", script :: keys.length.encode :: keys ::: args))

  def bitcount[F[_]: Concurrent](key: String): Redis[F, Long] = 
    runRequestTotal(NEL.of("BITCOUNT", key.encode))

  def bitcountrange[F[_]: Concurrent](key: String, start: Long, end: Long): Redis[F, Long] = 
    runRequestTotal(NEL.of("BITCOUNT", key.encode, start.encode, end.encode))


  private def bitop[F[_]: Concurrent](operation: String, keys: List[String]): Redis[F, Long]= 
    runRequestTotal(NEL("BITOP", operation :: keys))

  def bitopand[F[_]: Concurrent](destkey: String, srckeys: List[String]): Redis[F, Long] = 
    bitop("AND", destkey :: srckeys)

  def bitopor[F[_]: Concurrent](destkey: String, srckeys: List[String]): Redis[F, Long] = 
    bitop("OR", destkey :: srckeys)

  def bitopxor[F[_]: Concurrent](destkey: String, srckeys: List[String]): Redis[F, Long] = 
    bitop("XOR", destkey :: srckeys)

  def bitopnot[F[_]: Concurrent](destkey: String, srckey: String): Redis[F, Long] = 
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

  def set[F[_]: Concurrent](key: String, value: String, setOpts: SetOpts = SetOpts.default): Redis[F, Status] = {
    val ex = setOpts.setSeconds.toList.flatMap(l => List("EX", l.encode))
    val px = setOpts.setMilliseconds.toList.flatMap(l => List("PX", l.encode))
    val condition = setOpts.setCondition.toList.map(_.encode)
    val keepTTL = Alternative[List].guard(setOpts.keepTTL).as("KEEPTTL")
    runRequestTotal(NEL("SET", key.encode :: value.encode :: ex ::: px ::: condition ::: keepTTL))
  }

  final case class ZAddOpts(
    condition: Option[Condition],
    change: Boolean,
    increment: Boolean
  )
  object ZAddOpts {
    val default = ZAddOpts(None, false, false)
  }

  def zadd[F[_]: Concurrent](key: String, scoreMember: List[(Double, String)], options: ZAddOpts = ZAddOpts.default): Redis[F, Long] = {
    val scores = scoreMember.flatMap{ case (x, y) => List(x.encode, y.encode)}
    val condition = options.condition.toList.map(_.encode)
    val change = Alternative[List].guard(options.change).as("CH")
    val increment = Alternative[List].guard(options.increment).as("INCR")
    runRequestTotal(NEL("ZADD", key :: condition ::: change ::: increment ::: scores))
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

  def clientreply[F[_]: Concurrent](mode: ReplyMode): Redis[F, Boolean] = 
    runRequestTotal(NEL.of("CLIENT REPLY", mode.encode))

  def srandmember[F[_]: Concurrent](key: String): Redis[F, Option[String]] = 
    runRequestTotal(NEL.of("SRANDMEMBER", key.encode))

  def srandmemberMulti[F[_]: Concurrent](key: String, count: Long): Redis[F, List[String]] = 
    runRequestTotal(NEL.of("SRANDMEMBER", key.encode, count.encode))

  def spop[F[_]: Concurrent](key: String): Redis[F, Option[String]] = 
    runRequestTotal(NEL.of("SPOP", key.encode))

  def spopMulti[F[_]: Concurrent](key: String,  count: Long): Redis[F, List[String]] = 
    runRequestTotal(NEL.of("SPOP", key.encode, count.encode))

  def info[F[_]: Concurrent]: Redis[F, String] = 
    runRequestTotal(NEL.of("INFO"))

  def infosection[F[_]: Concurrent](section: String): Redis[F, String] = 
    runRequestTotal(NEL.of("INFO", section.encode))

  def exists[F[_]: Concurrent](key: String): Redis[F, Boolean] = 
    runRequestTotal(NEL.of("EXISTS", key.encode))
  
  // TODO Scan
  // TODO LEX
  // TODO xadd
  // TODO xread

  def xgroupcreate[F[_]: Concurrent](stream: String, groupName: String, startId: String): Redis[F, Status] = 
    runRequestTotal(NEL.of("XGROUP", "CREATE", stream, groupName, startId))

  def xgroupsetid[F[_]: Concurrent](stream: String, groupName: String, messageId: String): Redis[F, Status] = 
    runRequestTotal(NEL.of("XGROUP", "SETID", stream, groupName, messageId))

  def xgroupdelconsumer[F[_]: Concurrent](stream: String, groupName: String, consumer: String): Redis[F, Long] = 
    runRequestTotal(NEL.of("XGROUP", "DELCONSUMER", stream, groupName, consumer))

  def xgroupdestroy[F[_]: Concurrent](stream: String, groupName: String): Redis[F, Boolean] =
    runRequestTotal(NEL.of("XGROUP", "DESTROY", stream, groupName))

  def xack[F[_]: Concurrent](stream: String, groupName: String, messageIds: List[String]): Redis[F, Long] = 
    runRequestTotal(NEL("XACK", stream :: groupName :: messageIds))

  // TODO xrange
  // TODO xrevrange

  def xlen[F[_]: Concurrent](stream: String): Redis[F, Long] = 
    runRequestTotal(NEL.of("XLEN", stream))
  
  // TODO xpendingsummary
  // TOOD xpendingdetail
  // TODO xclaim
  // TODO xinfo

  def xdel[F[_]: Concurrent](stream: String, messageIds: List[String]): Redis[F, Long] = 
    runRequestTotal(NEL("XDEL", stream :: messageIds))

  // TODO xtrim

  // Simple String Commands

  def ping[F[_]: Concurrent]: Redis[F, Status] =
    runRequestTotal[F, Status](NEL.of("PING"))

  def ttl[F[_]: Concurrent](key: String): Redis[F, Long] = 
    runRequestTotal[F, Long](NEL.of("TTL", key.encode))

  def setnx[F[_]: Concurrent](key: String, value: String): Redis[F, Boolean] = 
    runRequestTotal(NEL.of("SETNX", key.encode, value.encode))

  def pttl[F[_]: Concurrent](key: String): Redis[F, Long] =
    runRequestTotal(NEL.of("PTTL", key.encode))

  def commandcount[F[_]: Concurrent]: Redis[F, Long] = 
    runRequestTotal(NEL.of("COMMAND", "COUNT"))

  def clientsetname[F[_]: Concurrent](connectionName: String): Redis[F, String] = 
    runRequestTotal(NEL.of("CLIENT", "SETNAME", connectionName.encode))

  def zrank[F[_]: Concurrent](key: String, member: String): Redis[F, Long] = 
    runRequestTotal(NEL.of("ZRANK", key.encode, member.encode))

  def zremrangebyscore[F[_]: Concurrent](key: String, min: Double, max: Double): Redis[F, Long] = 
    runRequestTotal(NEL.of("ZREMRANGEBYSCORE", key.encode, min.encode, max.encode))

  def hkeys[F[_]: Concurrent](key: String): Redis[F, List[String]] = 
    runRequestTotal(NEL.of("HKEYS", key.encode))

  def slaveof[F[_]: Concurrent](host: String, port: Int): Redis[F, Status] = 
    runRequestTotal(NEL.of("SLAVEOF", host.encode, port.encode))

  def rpushx[F[_]: Concurrent](key: String, value: String): Redis[F, Long] = 
    runRequestTotal(NEL.of("RPUSHX", key.encode, value.encode))

  def debugobject[F[_]: Concurrent](key: String): Redis[F, String] = 
    runRequestTotal(NEL.of("DEBUG", "OBJECT", key.encode))

  def bgsave[F[_]: Concurrent]: Redis[F, Status] = 
    runRequestTotal(NEL.of("BGSAVE"))

  def hlen[F[_]: Concurrent](key: String): Redis[F, Long] = 
    runRequestTotal(NEL.of("HLEN", key.encode))

  def rpoplpush[F[_]: Concurrent](source: String, destination: String): Redis[F, Option[String]] = 
    runRequestTotal(NEL.of("RPOPLPUSH", source.encode, destination.encode))

  def brpop[F[_]: Concurrent](key: List[String], timeout: Long): Redis[F, Option[(String, String)]] = 
    runRequestTotal(NEL.of("BRPOP", (key.map(_.encode) ++ List(timeout.encode)):_*))

  def bgrewriteaof[F[_]: Concurrent]: Redis[F, Status] = 
    runRequestTotal(NEL.of("BGREWRITEAOF"))

  def zincrby[F[_]: Concurrent](key: String, increment: Long, member: String): Redis[F, Double] = 
    runRequestTotal(NEL.of("ZINCRBY", key.encode, increment.encode, member.encode))

  def hgetall[F[_]: Concurrent](key: String): Redis[F, List[(String, String)]] = 
    runRequestTotal(NEL.of("HGETALL", key.encode))

  def hmset[F[_]: Concurrent](key: String, fieldValue: List[(String, String)]): Redis[F, Status] = 
    runRequestTotal(NEL("HMSET", key.encode :: fieldValue.flatMap{ case (x, y) => List(x.encode, y.encode)}))

  def sinter[F[_]: Concurrent](key: List[String]): Redis[F, List[String]] = 
    runRequestTotal(NEL("SINTER", key.map(_.encode)))

  def pfadd[F[_]: Concurrent](key: String, value: List[String]): Redis[F, Long] = 
    runRequestTotal(NEL("PFADD", key.encode :: value.map(_.encode)))

  def zremrangebyrank[F[_]: Concurrent](key: String, start: Long, stop: Long): Redis[F, Long] = 
    runRequestTotal(NEL.of("ZREMRANGEBYRANK", key.encode, start.encode, stop.encode))

  def flushdb[F[_]: Concurrent]: Redis[F, Status] = 
    runRequestTotal(NEL.of("FLUSHDB"))

  def sadd[F[_]: Concurrent](key: String, member: List[String]): Redis[F, Long] = 
    runRequestTotal(NEL("SADD", key.encode :: member.map(_.encode)))

  def lindex[F[_]: Concurrent](key: String, index: Int): Redis[F, Option[String]] = 
    runRequestTotal(NEL.of("LINDEX", key.encode, index.encode))

  def lpush[F[_]: Concurrent](key: String, value: List[String]): Redis[F, Long] = 
    runRequestTotal(NEL("LPUSH", key.encode :: value.map(_.encode)))

  def hstrlen[F[_]: Concurrent](key: String, field: String): Redis[F, Long] = 
    runRequestTotal(NEL.of("HSTRLEN", key.encode, field.encode))

  def smove[F[_]: Concurrent](source: String, destination: String, member: String): Redis[F, Boolean] = 
    runRequestTotal(NEL.of("SMOVE", source.encode, destination.encode, member.encode))

  def zscore[F[_]: Concurrent](key: String, member: String): Redis[F, Option[Double]] = 
    runRequestTotal(NEL.of("ZSCORE", key.encode, member.encode))

  def configresetstat[F[_]: Concurrent]: Redis[F, Status] = 
    runRequestTotal(NEL.of("CONFIG", "RESETSTAT"))

  def pfcount[F[_]: Concurrent](key: List[String]): Redis[F, Long] = 
    runRequestTotal(NEL("PFCOUNT", key.map(_.encode)))

  def hdel[F[_]: Concurrent](key: String, field: List[String]): Redis[F, Long] = 
    runRequestTotal(NEL("HDEL", key.encode :: field.map(_.encode)))

  def incrbyfloat[F[_]: Concurrent](key: String, increment: Double): Redis[F, Double] = 
    runRequestTotal(NEL.of("INCRBYFLOAT", key.encode, increment.encode))

  def setbit[F[_]: Concurrent](key: String, offset: Long, value: String): Redis[F, Long] =
    runRequestTotal(NEL.of("SETBIT", key.encode, offset.encode, value.encode))

  def flushall[F[_]: Concurrent]: Redis[F, Status] = 
    runRequestTotal(NEL.of("FLUSHALL"))

  def incrby[F[_]: Concurrent](key: String, increment: Long): Redis[F, Long] = 
    runRequestTotal(NEL.of("INCRBY", key.encode, increment.encode))

  def time[F[_]: Concurrent]: Redis[F, (Long, Long)] = 
    runRequestTotal(NEL.of("TIME"))

  def smembers[F[_]: Concurrent](key: String): Redis[F, List[String]] = 
    runRequestTotal(NEL.of("SMEMBERS", key.encode))

  def zlexcount[F[_]: Concurrent](key: String, min: String, max: String): Redis[F, Long] = 
    runRequestTotal(NEL.of("ZLEXCOUNT", key.encode, min.encode, max.encode))

  def sunion[F[_]: Concurrent](key: List[String]): Redis[F, List[String]] = 
    runRequestTotal(NEL("SUNION", key.map(_.encode)))

  def sinterstore[F[_]: Concurrent](destination: String, key: List[String]): Redis[F, Long] = 
    runRequestTotal(NEL("SINTERSTORE", destination.encode :: key.map(_.encode)))

  def hvals[F[_]: Concurrent](key: String): Redis[F, List[String]] = 
    runRequestTotal(NEL.of("HVALS", key.encode))

  def configset[F[_]: Concurrent](parameter: String, value: String): Redis[F, Status] = 
    runRequestTotal(NEL.of("CONFIG", "SET", parameter.encode, value.encode))

  def scriptflush[F[_]: Concurrent]: Redis[F, Status] = 
    runRequestTotal(NEL.of("SCRIPT", "FLUSH"))

  def dbsize[F[_]: Concurrent]: Redis[F, Long] = 
    runRequestTotal(NEL.of("DBSIZE"))

  def wait[F[_]: Concurrent](numslaves: Long, timeout: Long): Redis[F, Long] = 
    runRequestTotal(NEL.of("WAIT", numslaves.encode, timeout.encode))

  def lpop[F[_]: Concurrent](key: String): Redis[F, Option[String]] = 
    runRequestTotal(NEL.of("LPOP", key.encode))

  def clientpause[F[_]: Concurrent](timeout: Long): Redis[F, Status] = 
    runRequestTotal(NEL.of("CLIENT", "PAUSE", timeout.encode))

  def expire[F[_]: Concurrent](key: String, seconds: Long): Redis[F, Boolean] = 
    runRequestTotal(NEL.of("EXPIRE", key.encode, seconds.encode))

  def mget[F[_]: Concurrent](key: List[String]): Redis[F, List[Option[String]]] = 
    runRequestTotal(NEL("MGET", key.map(_.encode)))

  def bitpos[F[_]: Concurrent](key: String, bit: Long, start: Long, end: Long): Redis[F, Long] = 
    runRequestTotal(NEL.of("BITPOS", key.encode, bit.encode, start.encode, end.encode))

  def lastsave[F[_]: Concurrent]: Redis[F, Long] = 
    runRequestTotal(NEL.of("LASTSAVE"))

  def pexpire[F[_]: Concurrent](key: String, milliseconds: Long): Redis[F, Boolean] = 
    runRequestTotal(NEL.of("PEXPIRE", key.encode, milliseconds.encode))

  def clientlist[F[_]: Concurrent]: Redis[F, List[String]] = 
    runRequestTotal(NEL.of("CLIENT", "LIST"))

  def renamenx[F[_]: Concurrent](key: String, newkey: String): Redis[F, Boolean] = 
    runRequestTotal(NEL.of("RENAMENX", key.encode, newkey.encode))

  def pfmerge[F[_]: Concurrent](destkey: String, sourcekey: List[String]): Redis[F, String] = 
    runRequestTotal(NEL("PFMERGE", destkey.encode :: sourcekey.map(_.encode)))

  def lrem[F[_]: Concurrent](key: String, count: Long, value: String): Redis[F, Long] = 
    runRequestTotal(NEL.of("LREM", key.encode, count.encode, value.encode))

  def sdiff[F[_]: Concurrent](key: List[String]): Redis[F, List[String]] = 
    runRequestTotal(NEL("SDIFF", key.map(_.encode)))

  def get[F[_]: Concurrent](key: String): Redis[F, Option[String]] = 
    runRequestTotal(NEL.of("GET", key.encode))

  def getrange[F[_]: Concurrent](key: String, start: Long, end: Long): Redis[F, String] = 
    runRequestTotal(NEL.of("GETRANGE", key.encode, start.encode, end.encode))

  def sdiffstore[F[_]: Concurrent](destination: String, key: List[String]): Redis[F, Long] = 
    runRequestTotal(NEL("SDIFFSTORE", destination.encode :: key.map(_.encode)))

  def zcount[F[_]: Concurrent](key: String, min: Double, max: Double): Redis[F, Long] =
    runRequestTotal(NEL.of("ZCOUNT", key.encode, min.encode, max.encode))

  def scriptload[F[_]: Concurrent](script: String): Redis[F, String] =
    runRequestTotal(NEL.of("SCRIPT", "LOAD", script.encode))

  def getset[F[_]: Concurrent](key: String, value: String): Redis[F, Option[String]] = 
    runRequestTotal(NEL.of("GETSET", key.encode, value.encode))

  def dump[F[_]: Concurrent](key: String): Redis[F, String] = 
    runRequestTotal(NEL.of("DUMP", key.encode))

  def keys[F[_]: Concurrent](pattern: String): Redis[F, List[String]] = 
    runRequestTotal(NEL.of("KEYS", pattern.encode))

  def configget[F[_]: Concurrent](parameter: String): Redis[F, List[(String, String)]] = 
    runRequestTotal(NEL.of("CONFIG", "GET", parameter.encode))

  def rpush[F[_]: Concurrent](key: String, value: List[String]): Redis[F, Long] = 
    runRequestTotal(NEL("RPUSH", key.encode :: value.map(_.encode)))

  def randomkey[F[_]: Concurrent]: Redis[F, Option[String]] = 
    runRequestTotal(NEL.of("RANDOMKEY"))

  def hsetnx[F[_]: Concurrent](key: String, field: String, value: String): Redis[F, Boolean] = 
    runRequestTotal(NEL.of("HSETNX", key.encode, field.encode, value.encode))

  def mset[F[_]: Concurrent](keyvalue: List[(String, String)]): Redis[F, Status] = 
    runRequestTotal(NEL("MSET", keyvalue.flatMap{ case (x,y) => List(x.encode, y.encode)}))

  def setex[F[_]: Concurrent](key: String, seconds: Long, value: String): Redis[F, Status] = 
    runRequestTotal(NEL.of(key.encode, seconds.encode, value.encode))

  def psetex[F[_]: Concurrent](key: String, milliseconds: Long, value: String): Redis[F, Status] = 
    runRequestTotal(NEL.of("PSETEX", key.encode, milliseconds.encode, value.encode))

  def scard[F[_]: Concurrent](key: String): Redis[F, Long] = 
    runRequestTotal(NEL.of("SCARD", key.encode))

  def scriptexists[F[_]: Concurrent](script: List[String]): Redis[F, List[Boolean]] = 
    runRequestTotal(NEL("SCRIPT", "EXISTS" :: script.map(_.encode)))

  def sunionstore[F[_]: Concurrent](destination: String, key: List[String] ): Redis[F, Long] = 
    runRequestTotal(NEL("SUNIONSTORE", destination.encode :: key.map(_.encode)))

  def persist[F[_]: Concurrent](key: String): Redis[F, Boolean] = 
    runRequestTotal(NEL.of("PERSIST", key.encode))

  def strlen[F[_]: Concurrent](key: String): Redis[F, Long] = 
    runRequestTotal(NEL.of("STRLEN", key.encode))

  def lpushx[F[_]: Concurrent](key: String, value: String): Redis[F, Long] = 
    runRequestTotal(NEL.of("LPUSHX", key.encode, value.encode))

  def hset[F[_]: Concurrent](key: String, field: String, value: String): Redis[F, Boolean] = 
    runRequestTotal(NEL.of("HSET", key.encode, field.encode, value.encode))

  def brpoplpush[F[_]: Concurrent](source: String, destination: String, timeout: Long): Redis[F, Option[String]] = 
    runRequestTotal(NEL.of("BRPOPLPUSH", source.encode, destination.encode, timeout.encode))

  def zrevrank[F[_]: Concurrent](key: String, member: String): Redis[F, Option[Long]] = 
    runRequestTotal(NEL.of("ZREVRANK", key.encode, member.encode))

  def scriptkill[F[_]: Concurrent]: Redis[F, Status] = 
    runRequestTotal(NEL.of("SCRIPT", "KILL"))

  def setrange[F[_]: Concurrent](key: String, offset: Long, value: String): Redis[F, Long] = 
    runRequestTotal(NEL.of("SETRANGE", key.encode, offset.encode, value.encode))

  def del[F[_]: Concurrent](key: List[String]): Redis[F, Long] = 
    runRequestTotal(NEL("DEL", key.map(_.encode)))

  def hincrbyfloat[F[_]: Concurrent](key: String,field: String,increment: Double): Redis[F, Double] = 
    runRequestTotal(NEL.of("HINCRBYFLOAT", key.encode, field.encode, increment.encode))


  def hincrby[F[_]: Concurrent](key: String, field: String, increment: Long): Redis[F, Long] = 
    runRequestTotal(NEL.of("HINCRBY", key.encode, field.encode, increment.encode))

  def zremrangebylex[F[_]: Concurrent](key: String, min: String, max: String): Redis[F, Long]  =
    runRequestTotal(NEL.of("ZREMRANGEBYLEX", key.encode, min.encode, max.encode))

  def rpop[F[_]: Concurrent](key: String): Redis[F, Option[String]] = 
    runRequestTotal(NEL.of("RPOP", key.encode))

  def rename[F[_]: Concurrent](key: String, newkey: String): Redis[F, Status] = 
    runRequestTotal(NEL.of("RENAME", key.encode, newkey.encode))

  def zrem[F[_]: Concurrent](key: String, member: List[String]): Redis[F, Long] = 
    runRequestTotal(NEL("ZREM", key.encode :: member.map(_.encode)))

  def hexists[F[_]: Concurrent](key: String, field: String): Redis[F, Boolean] =
    runRequestTotal(NEL.of("HEXISTS", key.encode, field.encode))

  def clientgetname[F[_]: Concurrent]: Redis[F, Status] = 
    runRequestTotal(NEL.of("CLIENT", "GETNAME"))

  def configerewrite[F[_]: Concurrent]: Redis[F, Status] = 
    runRequestTotal(NEL.of("CONFIG", "REWRITE"))

  def decr[F[_]: Concurrent](key: String): Redis[F, Long] =
    runRequestTotal(NEL.of("DECR", key.encode))

  def hmget[F[_]: Concurrent](key: String, field: List[String]): Redis[F, List[Option[String]]] = 
    runRequestTotal(NEL("HMGET", key.encode :: field.map(_.encode)))

  def lrange[F[_]: Concurrent](key: String, start: Long, stop: Long): Redis[F, List[String]] = 
    runRequestTotal(NEL.of("LRANGE", key.encode, start.encode, stop.encode))

  def decrby[F[_]: Concurrent](key: String, decrement: Long): Redis[F, Long] = 
    runRequestTotal(NEL.of("DECRBY", key.encode, decrement.encode))

  def llen[F[_]: Concurrent](key: String): Redis[F, Long] = 
    runRequestTotal(NEL.of("LLEN", key.encode))

  def append[F[_]: Concurrent](key: String, value: String): Redis[F, Long] = 
    runRequestTotal(NEL.of("APPEND", key.encode, value.encode))

  def incr[F[_]: Concurrent](key: String): Redis[F, Long] =
    runRequestTotal(NEL.of("INCR", key.encode))

  def hget[F[_]: Concurrent](key: String, field: String): Redis[F, Option[String]] = 
    runRequestTotal(NEL.of("HGET", key.encode, field.encode))

  def pexpireat[F[_]: Concurrent](key: String, milliseconds: Long): Redis[F, Boolean] = 
    runRequestTotal(NEL.of("PEXPIREAT", key.encode, milliseconds.encode))

  def ltrim[F[_]: Concurrent](key: String, start: Long, stop: Long): Redis[F, Status] = 
    runRequestTotal(NEL.of("LTRIM", key.encode, start.encode, stop.encode))

  def zcard[F[_]: Concurrent](key: String): Redis[F, Long] = 
    runRequestTotal(NEL.of("ZCARD", key.encode))

  def lset[F[_]: Concurrent](key: String, index: Long, value: String): Redis[F, Status] = 
    runRequestTotal(NEL.of("LSET", key.encode, index.encode, value.encode))

  def expireat[F[_]: Concurrent](key: String, timestamp: Long): Redis[F, Boolean] =
    runRequestTotal(NEL.of("EXPIREAT", key.encode, timestamp.encode))

  def save[F[_]: Concurrent]: Redis[F, Status] = 
    runRequestTotal(NEL.of("SAVE"))

  def move[F[_]: Concurrent](key: String, db: Long): Redis[F, Boolean] = 
    runRequestTotal(NEL.of("MOVE", key.encode, db.encode))

  def getbit[F[_]: Concurrent](key: String, offset: Long): Redis[F, Long] = 
    runRequestTotal(NEL.of("GETBIT", key.encode, offset.encode))

  def msetnx[F[_]: Concurrent](keyvalue: List[(String, String)]): Redis[F, Boolean] = 
    runRequestTotal(NEL("MSETNX", keyvalue.flatMap{case (x, y) => List(x.encode, y.encode)}))

  def commandinfo[F[_]: Concurrent](commandName: List[String]): Redis[F, List[String]] = 
    runRequestTotal(NEL("COMMAND", "INFO" :: commandName.map(_.encode)))

  def quit[F[_]: Concurrent]: Redis[F, Status] = 
    runRequestTotal(NEL.of("QUIT"))

  def blpop[F[_]: Concurrent](key: List[String], timeout: Long): Redis[F, Option[(String, String)]] = 
    runRequestTotal(NEL("BLPOP", key.map(_.encode) ++ List(timeout.encode)))

  def srem[F[_]: Concurrent](key: String, member: List[String]): Redis[F, Long] = 
    runRequestTotal(NEL("SREM", key.encode :: member.map(_.encode)))

  def echo[F[_]: Concurrent](message: String): Redis[F, String] = 
    runRequestTotal(NEL.of("ECHO", message.encode))

  def sismember[F[_]: Concurrent](key: String, member: String): Redis[F, Boolean] = 
    runRequestTotal(NEL.of("SISMEMBER", key.encode, member.encode))

}