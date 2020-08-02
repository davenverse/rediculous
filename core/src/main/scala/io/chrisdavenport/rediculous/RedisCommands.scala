package io.chrisdavenport.rediculous

import cats.effect._
import cats.data.{NonEmptyList => NEL}
import RedisProtocol._
import RedisConnection.runRequestTotal
import _root_.io.chrisdavenport.rediculous.implicits._

object RedisCommands {

  def ping[F[_]: Concurrent]: Redis[F, Status] =
    runRequestTotal[F, Status](NEL.of("PING"))

  def set[F[_]: Concurrent](key: String, value: String): Redis[F, Option[String]] = 
    runRequestTotal[F, Option[String]](NEL.of("SET", key.encode, value.encode))

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