package io.chrisdavenport.rediculous

import cats.syntax.all._
import cats.effect._
import munit.CatsEffectSuite
import scala.concurrent.duration._
import _root_.io.chrisdavenport.whaletail.Docker
import _root_.io.chrisdavenport.whaletail.manager._
import com.comcast.ip4s.Host
import com.comcast.ip4s.Port

class RedisCommandsSpec extends CatsEffectSuite {
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
  test("set/get parity"){ //connection => 
    redisConnection().flatMap{connection => 
      val key = "foo"
      val value = "bar"
      val action = RedisCommands.set[RedisIO](key, value) >> 
        RedisCommands.get[RedisIO](key) <* 
        RedisCommands.del[RedisIO]("foo")
      action.run(connection)
    }.map{
      assertEquals(_, Some("bar"))
    }
  }

  test("xadd/xread parity"){
    redisConnection().flatMap{ connection => 
      val kv = "bar" -> "baz"
      val action = RedisCommands.xadd[RedisIO]("foo", List(kv)) >>
        RedisCommands.xread[RedisIO](Set(RedisCommands.StreamOffset.All("foo"))) <*
        RedisCommands.del[RedisIO]("foo")

      val extract = (resp: Option[List[RedisCommands.XReadResponse]]) => 
        resp.flatMap(_.headOption).flatMap(_.records.headOption).flatMap(_.keyValues.headOption)

      action.run(connection).map{ resp => 
        assertEquals(extract(resp), Some(kv))
      }
    }
  }

  test("xadd/xtrim parity"){
    redisConnection().flatMap{ connection => 
      val kv = "bar" -> "baz"
      val action = RedisCommands.xadd[RedisIO]("foo", List(kv)).replicateA(10) >>
        RedisCommands.xtrim[RedisIO]("foo", RedisCommands.XTrimStrategy.MaxLen(2), RedisCommands.Trimming.Exact.some) <*
        RedisCommands.del[RedisIO]("foo")

      action.run(connection).assertEquals(8)
    }
  }

  test("xadd/xgroupread parity"){
    redisConnection().flatMap{ connection => 
      val msg1 = "msg1" -> "msg1"
      val msg2 = "msg2" -> "msg2"
      val msg3 = "msg3" -> "msg3"

      val extract = (resp: Option[List[RedisCommands.XReadResponse]]) => 
        resp.flatMap(_.headOption).flatMap(_.records.headOption).flatMap(_.keyValues.headOption)

      val action = 
        for {
          _ <- RedisCommands.xgroupcreate[RedisIO]("foo", "group1", "$", true)
          _ <- RedisCommands.xadd[RedisIO]("foo", List(msg1))
          _ <- RedisCommands.xadd[RedisIO]("foo", List(msg2))
          _ <- RedisCommands.xadd[RedisIO]("foo", List(msg3))
          msg1 <- RedisCommands.xreadgroup[RedisIO](RedisCommands.Consumer("group1", "consumer1"), Set(RedisCommands.StreamOffset.LastConsumed("foo")), RedisCommands.XReadOpts.default.copy(count = Some(1)))
          msg2 <- RedisCommands.xreadgroup[RedisIO](RedisCommands.Consumer("group1", "consumer2"), Set(RedisCommands.StreamOffset.LastConsumed("foo")), RedisCommands.XReadOpts.default.copy(count = Some(1)))
          msg3 <- RedisCommands.xreadgroup[RedisIO](RedisCommands.Consumer("group1", "consumer3"), Set(RedisCommands.StreamOffset.LastConsumed("foo")), RedisCommands.XReadOpts.default.copy(count = Some(1)))
          _ <- RedisCommands.xgroupdestroy[RedisIO]("foo", "group1")
          _ <- RedisCommands.del[RedisIO]("foo")
        } yield (extract(msg1), extract(msg2), extract(msg3))

      action.run(connection).assertEquals((Some(msg1), Some(msg2), Some(msg3)))
    }
  }

  test("xpendingsummary"){
    redisConnection().flatMap{ connection => 
      val msg1 = "msg1" -> "msg1"
      val msg2 = "msg2" -> "msg2"
      val msg3 = "msg3" -> "msg3"

      val action = 
        for {
          _ <- RedisCommands.xgroupcreate[RedisIO]("foo", "group1", "$", true)
          id1 <- RedisCommands.xadd[RedisIO]("foo", List(msg1))
          id2 <- RedisCommands.xadd[RedisIO]("foo", List(msg2))
          _ <- RedisCommands.xadd[RedisIO]("foo", List(msg3))
          _ <- RedisCommands.xreadgroup[RedisIO](RedisCommands.Consumer("group1", "consumer1"), Set(RedisCommands.StreamOffset.LastConsumed("foo")), RedisCommands.XReadOpts.default.copy(count = Some(1)))
          _ <- RedisCommands.xreadgroup[RedisIO](RedisCommands.Consumer("group1", "consumer2"), Set(RedisCommands.StreamOffset.LastConsumed("foo")), RedisCommands.XReadOpts.default.copy(count = Some(1)))
          actual <- RedisCommands.xpendingsummary[RedisIO]("foo", "group1")
          _ <- RedisCommands.xgroupdestroy[RedisIO]("foo", "group1")
          _ <- RedisCommands.del[RedisIO]("foo")
        } yield 
          assertEquals(
            actual, 
            RedisCommands.XPendingSummary(2, id1, id2, List(
              "consumer1" -> 1,
              "consumer2" -> 1,
            ))
          )

      action.run(connection)
    }
  }
  
  test("xclaimsummary"){
    import RedisCommands._

    redisConnection().flatMap{ connection => 
      val addMsg = xadd[RedisIO]("foo", List("msg" -> "msg"))
      val args = XClaimArgs(1)
      val action = 
        for {
          _ <- xgroupcreate[RedisIO]("foo", "group1", "$", true)
          id1 <- addMsg
          id2 <- addMsg
          id3 <- addMsg
          id4 <- addMsg
          _ <- xreadgroup[RedisIO](Consumer("group1", "consumer1"), Set(StreamOffset.LastConsumed("foo")), XReadOpts.default.copy(count = Some(1)))
          _ <- xreadgroup[RedisIO](Consumer("group1", "consumer2"), Set(StreamOffset.LastConsumed("foo")))
          actual <- xclaimsummary[RedisIO]("foo", Consumer("group1", "consumer1"), args, List(id2, id3, id4))
          _ <- xgroupdestroy[RedisIO]("foo", "group1")
          _ <- del[RedisIO]("foo")
        } yield assertEquals(actual, List(id2, id3, id4))
      action.run(connection)
    }
  }

  test("xautoclaimsummary"){
    import RedisCommands._

    redisConnection().flatMap{ connection => 
      val addMsg = xadd[RedisIO]("foo", List("msg" -> "msg"))
      val args = XAutoClaimArgs(Consumer("group1", "consumer1"), 1, "0-0", Some(100))
      val action = 
        for {
          _ <- xgroupcreate[RedisIO]("foo", "group1", "$", true)
          id1 <- addMsg
          id2 <- addMsg
          id3 <- addMsg
          id4 <- addMsg
          _ <- xreadgroup[RedisIO](Consumer("group1", "consumer1"), Set(StreamOffset.LastConsumed("foo")), XReadOpts.default.copy(count = Some(1)))
          _ <- xreadgroup[RedisIO](Consumer("group1", "consumer2"), Set(StreamOffset.LastConsumed("foo")))
          _ <- xdel[RedisIO]("foo", List(id4))
          actual <- xautoclaimsummary[RedisIO]("foo", args)
          _ <- xgroupdestroy[RedisIO]("foo", "group1")
          _ <- del[RedisIO]("foo")
        } yield assertEquals(actual, XAutoClaimSummary("0-0", List(id1, id2, id3), List(id4)))
      action.run(connection)
    }
  }

  test("early termination"){
    redisConnection().flatMap{ connection => 
      val msg1 = "msg1" -> "msg1"
      val msg2 = "msg2" -> "msg2"
      val msg3 = "msg3" -> "msg3"

      val xopts = 
        RedisCommands.XReadOpts.default
          // .copy(blockMillisecond = 0L.some, 1L.some)
          .copy(count = 1L.some, blockMillisecond = 1000L.some)

      val offset: Set[RedisCommands.StreamOffset] = Set(RedisCommands.StreamOffset.From("foo", "$"))

      val extract = (resp: Option[List[RedisCommands.XReadResponse]]) => 
        resp.flatMap(_.headOption).flatMap(_.records.headOption).flatMap(_.keyValues.headOption)

      val action = 
        for {
          _ <- (
            RedisCommands.xadd[RedisIO]("foo", List(msg1)),
            RedisCommands.xadd[RedisIO]("foo", List(msg2)),
            RedisCommands.xadd[RedisIO]("foo", List(msg3))
          ).tupled.run(connection)
          msg1 <- RedisCommands.xread[RedisIO](offset, xopts).run(connection).timeout(50.milli).attempt.map{
            case Right(resp) => extract(resp)
            case Left(_) => None
          }
          empty <- RedisCommands.xread[RedisIO](offset, xopts).run(connection).timeout(50.milli).replicateA(100).attempt
          _ <- RedisCommands.del[RedisIO]("foo").run(connection)
        } yield msg1

      action.assertEquals(None)
    }
  }
}
