
import io.chrisdavenport.rediculous._
import cats.implicits._
import cats.effect._
import fs2.io.net._
import fs2._
import com.comcast.ip4s._
import scala.concurrent.duration._
import cats.effect.std._

// Mimics 150 req/s load with 4 operations per request.
// Completes 1,000,000 redis operations
// Completes in <5 s
object BasicExample extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val r = for {
      // maxQueued: How many elements before new submissions semantically block. Tradeoff of memory to queue jobs. 
      // Default 1000 is good for small servers. But can easily take 100,000.
      // workers: How many threads will process pipelined messages.
      connection <- RedisConnection.queued[IO].withHost(host"localhost").withPort(port"6379").withMaxQueued(10000).withWorkers(workers = 1).build
    } yield connection

    r.use {client =>
      val x: Redis[IO, RedisProtocol.Status] =  RedisCommands.ping[RedisIO]
      val r = (
        x, 
        RedisCommands.get[RedisIO]("foo"),
        RedisCommands.set[RedisIO]("foo", "value"),
        RedisCommands.get[RedisIO]("foo")
      ).parTupled

      val r2= List.fill(10)(r.run(client)).parSequence

      val now = IO(java.time.Instant.now)
      (
        now,
        Stream(()).covary[IO].repeat.map(_ => Stream.evalSeq(r2)).parJoin(15).take(100000).compile.lastOrError.flatTap(Console[IO].println(_)),
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