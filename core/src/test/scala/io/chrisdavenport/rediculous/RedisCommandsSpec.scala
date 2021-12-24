package io.chrisdavenport.rediculous

import cats.syntax.all._
import cats.effect._
import munit.CatsEffectSuite
import scala.concurrent.duration._
import io.chrisdavenport.whaletail.Docker
import io.chrisdavenport.whaletail.manager._
import com.comcast.ip4s.Host
import com.comcast.ip4s.Port

class RedisCommandsSpec extends CatsEffectSuite {
  val resource = Docker.client[IO].flatMap(client => 
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
      (hostS, portI) <- Resource.eval(
        container.ports.get(6379).liftTo[IO](new Throwable("Missing Port"))
      )
      host <- Resource.eval(Host.fromString(hostS).liftTo[IO](new Throwable("Invalid Host")))
      port <- Resource.eval(Port.fromInt(portI).liftTo[IO](new Throwable("Invalid Port")))
      connection <- RedisConnection.pool[IO].withHost(host).withPort(port).build
    } yield connection 
    
  )
  // Not available on scala.js
  // val redisConnection = ResourceSuiteLocalFixture(
  //     "redisconnection",
  //     resource
  //   )
  // override def munitFixtures: Seq[Fixture[_]] = Seq(
  //   redisConnection
  // )
  val fixture = ResourceFixture(resource)
  fixture.test("set a value"){ connection => 
    // val connection = redisConnection()
    val key = "foo"
    val value = "bar"
    val action = RedisCommands.set[RedisIO](key, value) >> RedisCommands.get[RedisIO](key)
    action.run(connection).map{
      assertEquals(_, Some("bar"))
    }
  }
}
