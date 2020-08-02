package io.chrisdavenport.rediculous

import cats.effect._
import cats.data._
import RedisProtocol._
import _root_.io.chrisdavenport.rediculous.implicits._

object RedisCommands {
    

  def ping[F[_]: Concurrent]: Redis[F, Status] =
    RedisConnection.runRequestTotal[F, Status](NonEmptyList.of("PING"))

  def get[F[_]: Concurrent](key: String): Redis[F, Option[String]] = 
    RedisConnection.runRequestTotal[F, Option[String]](NonEmptyList.of("GET", key.encode))

  def set[F[_]: Concurrent](key: String, value: String): Redis[F, Option[String]] = 
    RedisConnection.runRequestTotal[F, Option[String]](NonEmptyList.of("SET", key.encode, value.encode))

}