import cats.effect._

object AuthExample extends IOApp.Simple {
    import io.chrisdavenport.rediculous._
    import com.comcast.ip4s._

    val host = sys.env.get("REDIS_HOST")
      .flatMap(Host.fromString)
      .getOrElse(host"localhost")
    val port = sys.env.get("REDIS_PORT")
      .flatMap(Port.fromString)
      .getOrElse(port"5432")
    val pass = sys.env.get("REDIS_PASS")
      .getOrElse(throw new RuntimeException("Missing REDIS_PASS variable"))

    val conn: Resource[IO, RedisConnection[IO]] =
      RedisConnection.queued[IO]
        .withHost(host)
        .withPort(port)
        .withMaxQueued(10000)
        .withWorkers(1)
        .withAuth(None, pass)
        .withTLS
        .build

    val auth = RedisCommands.auth[Redis[IO, *]](pass)
    val command = RedisCommands.ping[Redis[IO, *]]
    val prog = conn.use{ c =>
        command.run(c)
          .flatTap(IO.println)
    }

    def run = prog.void
}