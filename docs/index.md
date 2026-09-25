# rediculous - Pure FP Redis Client [![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.chrisdavenport/rediculous_2.13/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.chrisdavenport/rediculous_2.13)

REmote DIctionary Client, that's hysterical.

## Quick Start

```scala
libraryDependencies ++= Seq(
  "io.chrisdavenport" %% "rediculous" % "<version>"
)
```


```scala mdoc
import io.chrisdavenport.rediculous._
import cats.syntax.all._
import cats.effect._
import cats.effect.std.Console
import fs2.io.net._
import fs2._
import scala.concurrent.duration._
import com.comcast.ip4s._

// Mimics 150 req/s load with 4 operations per request.
// Completes 1,000,000 redis operations
// Completes in <5 s
object BasicExample extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val r = for {
      // maxQueued: How many elements before new submissions semantically block. Tradeoff of memory to queue jobs.
      // Default 1000 is good for small servers. But can easily take 100,000.
      // workers: How many threads will process pipelined messages.
      connection <- RedisConnection
        .queued[IO]
        .withHost(Host.fromString("localhost").get)
        .withPort(port"6379")
        .withMaxQueued(10000)
        .withWorkers(2)
        .build
    } yield connection

    r.use { client =>
      val r = (
        RedisCommands.ping[Redis[IO, *]],
        RedisCommands.get[Redis[IO, *]]("foo"),
        RedisCommands.set[Redis[IO, *]]("foo", "value"),
        RedisCommands.get[Redis[IO, *]]("foo")
      ).parTupled

      val r2 = List.fill(10)(r.run(client)).parSequence.map {
        _.flatMap { case (_, _, _, _) =>
          List((), (), (), ())
        }
      }

      val now = Clock[IO].realTimeInstant
      (
        now,
        Stream(())
          .covary[IO]
          .repeat
          .map(_ => Stream.evalSeq(r2))
          .parJoin(15)
          .take(1000000)
          .compile
          .drain,
        now
      ).mapN { case (before, _, after) =>
        (after.toEpochMilli() - before.toEpochMilli()).millis
      }.flatMap { duration =>
        Console[IO].println(s"Operation took ${duration}")
      }
    }.as(ExitCode.Success)
  }
}
```

## Transport security

TLS is **not** enabled by default. Redis itself listens in the clear by
default and most clients follow suit, so `rediculous` does too — but that
choice is worth making deliberately rather than inheriting.

On a plaintext connection everything is readable by anything on the path,
including the `AUTH` exchange. Credentials are sent as an ordinary RESP bulk
string, and the connection pool authenticates **every** socket it opens, not
just the first — so the password crosses the network repeatedly for the life
of the application.

Enable TLS with `.withTLS`:

```scala mdoc:compile-only
RedisConnection
  .queued[IO]
  .withHost(host"redis.example")
  .withPort(port"6379")
  .withAuth(None, "correct horse battery staple")
  .withTLS
  .build
```

`.withTLSContext` supplies your own `TLSContext` — for a private CA, say — and
`.withTLSParameters` tunes the handshake. Without a context, the system trust
store is used.

Use TLS whenever the connection leaves the host. A local Unix-socket or
loopback connection to a Redis on the same machine is a reasonable exception;
anything crossing a network is not, regardless of whether that network is
described as private.

Two notes for cluster connections. Slot redirects (`MOVED` and `ASK`) name the
node to retry against, and those names come from the server; `rediculous`
follows a redirect only to a node the cluster topology already lists, so a
redirect cannot send your credentials to an arbitrary host. That check does
nothing for the credentials already in flight on a plaintext link, which is
the stronger reason to enable TLS.
