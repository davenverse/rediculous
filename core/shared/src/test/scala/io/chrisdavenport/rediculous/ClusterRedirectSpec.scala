package io.chrisdavenport.rediculous

import com.comcast.ip4s._
import io.chrisdavenport.rediculous.cluster.ClusterCommands._

class ClusterRedirectSpec extends munit.FunSuite {

  private def server(h: String, p: Int, id: String): ClusterServer =
    ClusterServer(
      Host.fromString(h).getOrElse(fail(s"bad host $h")),
      Port.fromInt(p).getOrElse(fail(s"bad port $p")),
      id
    )

  private def hostPort(h: String, p: Int): (Host, Port) =
    (
      Host.fromString(h).getOrElse(fail(s"bad host $h")),
      Port.fromInt(p).getOrElse(fail(s"bad port $p"))
    )

  // Two slot ranges, each with a master and a replica.
  private val topology = ClusterSlots(
    List(
      ClusterSlot(0, 8191, List(server("10.0.0.1", 6379, "a"), server("10.0.0.2", 6379, "b"))),
      ClusterSlot(8192, 16383, List(server("10.0.0.3", 6379, "c"), server("10.0.0.4", 6379, "d")))
    )
  )

  test("clusterMembers lists every master and replica") {
    assertEquals(
      RedisConnection.clusterMembers(topology),
      Set(
        hostPort("10.0.0.1", 6379),
        hostPort("10.0.0.2", 6379),
        hostPort("10.0.0.3", 6379),
        hostPort("10.0.0.4", 6379)
      )
    )
  }

  test("a redirect to a master is followed") {
    assert(RedisConnection.redirectIsKnown(topology, hostPort("10.0.0.1", 6379)))
  }

  test("a redirect to a replica is followed") {
    assert(RedisConnection.redirectIsKnown(topology, hostPort("10.0.0.4", 6379)))
  }

  test("a redirect to a host outside the cluster is refused") {
    // The case that matters: a compromised node, or a response injected into a
    // plaintext connection, naming somewhere the credentials should not go.
    assert(!RedisConnection.redirectIsKnown(topology, hostPort("198.51.100.7", 6379)))
  }

  test("a redirect to a known host on a different port is refused") {
    // Port is part of the identity; a node is not any port on that machine.
    assert(!RedisConnection.redirectIsKnown(topology, hostPort("10.0.0.1", 16379)))
  }

  test("a redirect to loopback is refused when the cluster is not on loopback") {
    assert(!RedisConnection.redirectIsKnown(topology, hostPort("127.0.0.1", 6379)))
  }

  test("nothing is known when the topology is empty") {
    assertEquals(RedisConnection.clusterMembers(ClusterSlots(Nil)), Set.empty[(Host, Port)])
    assert(!RedisConnection.redirectIsKnown(ClusterSlots(Nil), hostPort("10.0.0.1", 6379)))
  }
}
