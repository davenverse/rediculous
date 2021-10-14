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
  val all = "__keyspace*__:*"
  val foo = "foo"

  def run(args: List[String]): IO[ExitCode] = {
    fs2.io.net.Network[IO].client(SocketAddress(host"localhost", port"6379")).use{
      s => 
        val alg = RedisPubSub.socket(s, {r => IO.println(s"other: $r")}, Ref.unsafe[IO, Map[String, RedisPubSub.PubSubMessage => IO[Unit]]](Map()))
        alg.psubscribe(all, {r => IO.println("p: " + r.toString())}) >>
        alg.subscribe(foo, {r  => IO.println("s: " + r.toString())}) >>
        alg.ping >>
        alg.runMessages
    }.as(ExitCode.Success)
    // val r = for {
    //   connection <- RedisConnection.pool[IO].withHost(host"localhost").withPort(port"30001").build
    // } yield connection

    // r.use {con =>
    //   RedisConnection.runRequestTotal[IO, ClusterCommands.ClusterSlots](NonEmptyList.of("CLUSTER", "SLOTS"), None).unRedis.run(con)
    //   .flatTap(r => IO(println(r)))
    // } >>
    //   IO.pure(ExitCode.Success)
    
  }

}