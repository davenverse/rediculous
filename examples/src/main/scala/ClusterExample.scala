import io.chrisdavenport.rediculous._
import cats.implicits._
import cats.effect._
import fs2.Stream
import fs2.io.tcp._
import scala.concurrent.duration._

// Send a Single Transaction to the Redis Server
object ClusterExample extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val r = for {
      blocker <- Blocker[IO]
      sg <- SocketGroup[IO](blocker)
      // maxQueued: How many elements before new submissions semantically block.
      // Default 1000 is good for small servers. But can easily take 100,000.
      connection <- RedisConnection.cluster[IO](sg, "localhost", 30001, maxQueued = 10000)
    } yield connection

    r.use {client =>
      def keyed(key: String) = (
        RedisCommands.ping[Redis[IO, *]],
        RedisCommands.del[Redis[IO, *]](key),
        RedisCommands.get[Redis[IO, *]](key),
        RedisCommands.set[Redis[IO, *]](key, "value"),
        RedisCommands.get[Redis[IO, *]](key)
      ).parTupled

      val r = (keyed("foo"), keyed("bar"), keyed("baz")).parTupled

      val r2= List.fill(10)(r.run(client)).parSequence.map{_.flatMap{
        case ((_,_,_,_, _), (_,_,_,_, _),(_,_,_,_, _)) => List((), (), (), (), (), (), (), (), (), (), (), (), (), (), ())
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
    }.as(ExitCode.Success)

  }
}