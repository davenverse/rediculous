package io.chrisdavenport.rediculous.cluster

import cats.syntax.all._
import scodec.bits.ByteVector

class HashSlotSpec extends munit.FunSuite {

  def toBV(bv: String): ByteVector = ByteVector.encodeUtf8(bv).fold(throw _, identity(_))
    test("HashSlot.hashKey Find the right key section for a keyslot"){
      val input = "{user.name}.foo"
      assert(HashSlot.hashKey(toBV(input)) === toBV("user.name"))
    }
    test("HashSlot.hashKey Find the right key in middle of key") {
      val input = "bar{foo}baz"
      assert(HashSlot.hashKey(toBV(input)) === toBV("foo"))
    }
    test("HashSlot.hashKey Find the right key at end of key"){
      val input = "barbaz{foo}"
      assert(HashSlot.hashKey(toBV(input)) === toBV("foo"))
    }
    test("HashSlot.hashKey output original key if braces are directly next to each other"){
      val input = "{}.bar"
      assert(HashSlot.hashKey(toBV(input)) === toBV(input))
    }
    test("HashSlot.hashKey output the full value if no keyslot present") {
      val input = "bazbarfoo"
      assert(HashSlot.hashKey(toBV(input)) === toBV(input))
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
  **/
  
}