package io.chrisdavenport.rediculous

import cats.syntax.all._
import cats.effect._
import munit.CatsEffectSuite
import scala.concurrent.duration._
import _root_.io.chrisdavenport.whaletail.Docker
import _root_.io.chrisdavenport.whaletail.manager._
import com.comcast.ip4s.Host
import com.comcast.ip4s.Port

class RedisStreamSpec extends CatsEffectSuite {
  val resource = Docker.default[IO].flatMap(client => 
    WhaleTailContainer.build(client, "redis", "latest".some, Map(6379 -> None), Map.empty, Map.empty)
      .evalTap(
        ReadinessStrategy.checkReadiness(
          client,
          _, 
          ReadinessStrategy.LogRegex(".*Ready to accept connections.*\\s".r),
          30.seconds
        )
      )
  ).flatMap(container => 
    for {
      t <- Resource.eval(
        container.ports.get(6379).liftTo[IO](new Throwable("Missing Port"))
      )
      (hostS, portI) = t
      host <- Resource.eval(Host.fromString(hostS).liftTo[IO](new Throwable("Invalid Host")))
      port <- Resource.eval(Port.fromInt(portI).liftTo[IO](new Throwable("Invalid Port")))
      connection <- RedisConnection.pool[IO].withHost(host).withPort(port).build
    } yield connection 
    
  )
  // Not available on scala.js
  val redisConnection = UnsafeResourceSuiteLocalDeferredFixture(
      "redisconnection",
      resource
    )
  override def munitFixtures: Seq[Fixture[_]] = Seq(
    redisConnection
  )
  test("send a single message"){ //connection => 
    val messages = List(
      RedisStream.XAddMessage("foo", List("bar" -> "baz", "zoom" -> "zad"))
    )
    redisConnection().flatMap{connection => 
      
      val rStream = RedisStream.fromConnection(connection)
      rStream.append(messages) >>
      rStream.read(Set("foo"), 512).take(1).compile.lastOrError

    }.map{ xrr => 
      val i = xrr.stream
      assertEquals(xrr.stream, "foo")
      val i2 = xrr.records.flatMap(sr => sr.keyValues)
      assertEquals(i2, messages.flatMap(_.body))
    }
  }

}


