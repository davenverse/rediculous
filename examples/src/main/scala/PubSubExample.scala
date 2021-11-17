import io.chrisdavenport.rediculous._
import cats.implicits._
import cats.data._
import cats.effect._
import fs2.io.net._
import com.comcast.ip4s._
import io.chrisdavenport.rediculous.cluster.ClusterCommands
import scala.concurrent.duration._

/**
 * Test App For Development Purposes
 **/
object PubSubExample extends IOApp {
  val all = "__keyspace*__:*"
  val foo = "foo"

  def run(args: List[String]): IO[ExitCode] = {
    RedisConnection.queued[IO].withHost(host"localhost").withPort(port"6379").withMaxQueued(10000).withWorkers(workers = 1).build.flatMap{
      connection => 
        RedisPubSub.fromConnection(connection, 4096)
    }.use{ alg => 
        alg.nonMessages({r => IO.println(s"other: $r")}) >>
        alg.unhandledMessages({r => IO.println(s"unhandled: $r")}) >>
        alg.psubscribe(all, {r => IO.println("p: " + r.toString())}) >>
        alg.subscribe(foo, {r  => IO.println("s: " + r.toString())}) >> {
          (
            alg.runMessages,
            Temporal[IO].sleep(10.seconds) >> 
            alg.subscriptions.flatTap(IO.println(_)) >> 
            alg.psubscriptions.flatTap(IO.println(_))
          ).parMapN{ case (_, _) => ()} 
        }
    }.as(ExitCode.Success)
    
  }

}