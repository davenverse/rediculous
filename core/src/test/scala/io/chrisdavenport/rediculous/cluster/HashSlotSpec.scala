package io.chrisdavenport.rediculous.cluster

import cats.effect.testing.specs2.CatsIO

class HashSlotSpec extends org.specs2.mutable.Specification with org.specs2.ScalaCheck with CatsIO {
  "HashSlot.hashKey" should {
    "Find the right key section for a keyslot" in {
      val input = "{user.name}.foo"
      HashSlot.hashKey(input) must_=== "user.name"
    }
    "Find the right key in middle of key" in {
      val input = "bar{foo}baz"
      HashSlot.hashKey(input) must_=== "foo"
    }
    "Find the right key at end of key" in {
      val input = "barbaz{foo}"
      HashSlot.hashKey(input) must_=== "foo"
    }
    "output original key if braces are directly next to each other" in {
      val input = "{}.bar"
      HashSlot.hashKey(input) must_=== input
    }
    "output the full value if no keyslot present" in {
      val input = "bazbarfoo"
      HashSlot.hashKey(input) must_=== input
    }
  }

  /**
  import cats.implicits._
  import cats.data.NonEmptyList
  import cats.effect._
  import fs2.io.tcp.SocketGroup
  import io.chrisdavenport.rediculous._
  import java.net.InetSocketAddress

  "IT" should {
    "get the same key as Redis" in prop {s: String => 
      val r = for {
        blocker <- Blocker[IO]
        sg <- SocketGroup[IO](blocker)
        connection <- RedisConnection.pool[IO](sg, new InetSocketAddress("localhost", 30001))
      } yield connection
    
      r.use{ con => 
        RedisConnection.runRequestTotal[IO, Int](NonEmptyList.of("CLUSTER", "KEYSLOT", s)).unRedis.run(con).flatten
      }.map{
        out => HashSlot.find(s) must_=== out
      }
    }
  }
  **/
  
}