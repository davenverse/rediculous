# rediculous - Pure FP Redis Client [![Build Status](https://travis-ci.com/ChristopherDavenport/rediculous.svg?branch=master)](https://travis-ci.com/ChristopherDavenport/rediculous) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.chrisdavenport/rediculous_2.13/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.chrisdavenport/rediculous_2.13)

REmote DIctionary Client, that's hysterical.

## Quick Start

To use rediculous in an existing SBT project with Scala 2.11 or a later version, add the following dependencies to your
`build.sbt` depending on your needs:

```scala
libraryDependencies ++= Seq(
  "io.chrisdavenport" %% "rediculous" % "<version>"
)
```


```scala
import io.chrisdavenport.rediculous._
import cats.implicits._
import cats.effect._
import fs2.io.tcp._
import java.net.InetSocketAddress
import fs2._
import scala.concurrent.duration._

// Mimics 150 req/s load with 4 operations per request.
// Completes 1,000,000 redis operations
// Completes in <5 s
object BasicExample extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val r = for {
      blocker <- Blocker[IO]
      sg <- SocketGroup[IO](blocker)
      // maxQueued: How many elements before new submissions semantically block. Tradeoff of memory to queue jobs. 
      // Default 1000 is good for small servers. But can easily take 100,000.
      // workers: How many threads will process pipelined messages.
      connection <- RedisConnection.queued[IO](sg, "localhost", 6379, maxQueued = 10000, workers = 2)
    } yield connection

    r.use {client =>
      val r = (
        RedisCommands.ping[Redis[IO, *]],
        RedisCommands.get[Redis[IO, *]]("foo"),
        RedisCommands.set[Redis[IO, *]]("foo", "value"),
        RedisCommands.get[Redis[IO, *]]("foo")
      ).parTupled

      val r2= List.fill(10)(r.run(client)).parSequence.map{_.flatMap{
        case (_,_,_,_) => List((), (), (), ())
      }}

      val now = IO(java.time.Instant.now)
      (
        now,
        Stream(()).covary[IO].repeat.map(_ => Stream.evalSeq(r2)).parJoin(15).take(1000000).compile.drain,
        now
      ).mapN{
        case (before, _, after) => (after.toEpochMilli() - before.toEpochMilli()).millis
      }.flatMap{ duration => 
        IO(println(s"Operation took ${duration}"))
      }
    } >>
      IO.pure(ExitCode.Success)
    
  }

}
```
