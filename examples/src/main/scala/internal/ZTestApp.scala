import io.chrisdavenport.rediculous._
import cats.implicits._
// import cats.data._
import cats.effect._
import fs2.io.tcp._
import java.net.InetSocketAddress
import io.chrisdavenport.rediculous.cluster.ClusterCommands

object ZTestApp extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val r = for {
      blocker <- Blocker[IO]
      sg <- SocketGroup[IO](blocker)
      connection <- RedisConnection.pool[IO](sg, new InetSocketAddress("localhost", 30001))
    } yield connection

    r.use {con =>
      // RedisConnection.runRequestTotal[IO, ClusterCommands.ClusterSlots](NonEmptyList.of("CLUSTER", "SLOTS")).unRedis.run(con).flatten
        // .flatTap(r => IO(println(r)))
        ClusterCommands.clusterslots[Redis[IO, *]].unRedis.run(con).flatten
          .flatTap(r => IO(println(r)))
    } >>
      IO.pure(ExitCode.Success)
    
  }

}