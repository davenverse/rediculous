import io.chrisdavenport.rediculous._
import cats.implicits._
import cats.effect._
import fs2.io.net._
import com.comcast.ip4s._

// Send a Single Set of Pipelined Commands to the Redis Server
object PipelineExample extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val r = for {
      // maxQueued: How many elements before new submissions semantically block. Tradeoff of memory to queue jobs. 
      // Default 10000 is good for small servers. But can easily take 100,000.
      // workers: How many threads will process pipelined messages.
      connection <- RedisConnection.queued[IO].withHost(host"localhost").withPort(port"6379").withMaxQueued(maxQueued = 10000).withWorkers(workers = 2).build
    } yield connection

    r.use {client =>
      val r = (
        RedisCommands.ping[RedisPipeline],
        RedisCommands.del[RedisPipeline]("foo"),
        RedisCommands.get[RedisPipeline]("foo"),
        RedisCommands.set[RedisPipeline]("foo", "value"),
        RedisCommands.get[RedisPipeline]("foo")
      ).tupled

      val multi = r.pipeline[IO]

      multi.run(client).flatTap(output => IO(println(output)))

    }.as(ExitCode.Success)

  }
}