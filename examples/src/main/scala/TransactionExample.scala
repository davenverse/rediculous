import io.chrisdavenport.rediculous._
import cats.implicits._
import cats.effect._
import fs2.io.tcp._
import java.net.InetSocketAddress

// Send a Single Transaction to the Redis Server
object TransactionExample extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val r = for {
      blocker <- Blocker[IO]
      sg <- SocketGroup[IO](blocker)
      // maxQueued: How many elements before new submissions semantically block. Tradeoff of memory to queue jobs. 
      // Default 1000 is good for small servers. But can easily take 100,000.
      // workers: How many threads will process pipelined messages.
      connection <- RedisConnection.queued[IO](sg, new InetSocketAddress("localhost", 6379), maxQueued = 10000, workers = 2)
    } yield connection

    r.use {client =>
      val r = (
        RedisCommands.ping[RedisTransaction.Transaction],
        RedisCommands.del[RedisTransaction.Transaction](List("foo")),
        RedisCommands.get[RedisTransaction.Transaction]("foo"),
        RedisCommands.set[RedisTransaction.Transaction]("foo", "value"),
        RedisCommands.get[RedisTransaction.Transaction]("foo")
      ).tupled

      val multi = r.transact[IO]

      multi.run(client).flatTap(output => IO(println(output)))

    }.as(ExitCode.Success)

  }
}