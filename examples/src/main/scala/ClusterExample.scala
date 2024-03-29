import io.chrisdavenport.rediculous._
import cats.implicits._
import cats.effect._
import fs2.Stream
import fs2.io.net._
import com.comcast.ip4s._
import scala.concurrent.duration._

// Mimics 150 req/s load with 15 operations per request.
// Completes 1,000,000 redis operations
// Completes in <8 s
object ClusterExample extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val r = for {
      // maxQueued: How many elements before new submissions semantically block.
      // Default 1000 is good for small servers. But can easily take 100,000.
      connection <- RedisConnection.cluster[IO].withHost(host"localhost").withPort(port"30001").withMaxQueued(maxQueued = 10000).withWorkers(workers = 2).build
    } yield connection

    r.use {client =>
      def keyed(key: String) = (
        RedisCommands.ping[RedisIO],
        RedisCommands.del[RedisIO](key),
        RedisCommands.get[RedisIO](key),
        RedisCommands.set[RedisIO](key, "value"),
        RedisCommands.get[RedisIO](key)
      ).parTupled

      val r = (keyed("foo"), keyed("bar"), keyed("baz")).parTupled

      val r2= r.run(client).map{
        case ((_,_,_,_, _), (_,_,_,_, _),(_,_,_,_, _)) => // 3 x 5
          List.fill(15)(())
      }

      val now = IO(java.time.Instant.now)
      (
        now,
        Stream(()).covary[IO].repeat.map(_ => Stream.evalSeq(r2)).parJoin(150).take(1000000).compile.drain,
        now
      ).mapN{
        case (before, _, after) => (after.toEpochMilli() - before.toEpochMilli()).millis
      }.flatMap{ duration => 
        IO(println(s"Operation took ${duration}"))
      }
    }.as(ExitCode.Success)

  }
}