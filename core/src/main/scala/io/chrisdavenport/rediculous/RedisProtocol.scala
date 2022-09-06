package io.chrisdavenport.rediculous

object RedisProtocol {
  sealed trait Status
  object Status {
    case object Ok extends RedisProtocol.Status
    case object Pong extends RedisProtocol.Status
    case class Status(getStatus: String) extends RedisProtocol.Status
  }

  sealed trait RedisType
  object RedisType {
    case object None extends RedisType
    case object String extends RedisType
    case object Hash extends RedisType
    case object List extends RedisType
    case object Set extends RedisType
    case object Stream extends RedisType
    case object ZSet extends RedisType
  }

}