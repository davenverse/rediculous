import io.chrisdavenport.rediculous._
import cats.implicits._
import cats.data._
import cats.effect._
import fs2.io.net._
import com.comcast.ip4s._
import io.chrisdavenport.rediculous.cluster.ClusterCommands

/**
 * Test App For Development Purposes
 **/
object ZTestApp extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val r = for {
      connection <- RedisConnection.pool[IO](Network[IO], host"localhost", port"30001")
    } yield connection

    r.use {con =>
      RedisConnection.runRequestTotal[IO, ClusterCommands.ClusterSlots](NonEmptyList.of("CLUSTER", "SLOTS"), None).unRedis.run(con)
      .flatTap(r => IO(println(r)))
    } >>
      IO.pure(ExitCode.Success)
    
  }

}