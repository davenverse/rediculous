import io.chrisdavenport.rediculous._
import java.util.concurrent.TimeoutException
import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.util.Random
import cats.effect._
import cats.implicits._
import fs2._
import fs2.timeseries.{TimeStamped, TimeSeries}
import fs2.io.net._
import com.comcast.ip4s._

object StreamRate {
  def rate[A] =
    TimeStamped.withPerSecondRate[Option[Chunk[A]], Float](_.map(chunk => chunk.size.toFloat).getOrElse(0.0f))

  def averageRate[A] =
    rate[A].andThen(Scan.stateful1(Queue.empty[Float]) { 
      case (q, tsv @ TimeStamped(_, Right(_))) => (q, tsv)
      case (q, TimeStamped(t, Left(sample))) => 
        val q2 = (sample +: q).take(10)
        val average = q2.sum / q2.size
        (q, TimeStamped(t, Left(average)))
    })

  implicit class Logger[F[_]: Temporal, A](input: Stream[F, A]) {
    def logAverageRate(logger: Float => F[Unit]): Stream[F, A] =
      TimeSeries.timePulled(input.chunks, 1.second, 1.second)
        .through(averageRate.toPipe)
        .flatMap {
          case TimeStamped(_, Left(rate)) => Stream.exec(logger(rate))
          case TimeStamped(_, Right(Some(chunk))) => Stream.chunk(chunk)
          case TimeStamped(_, Right(None)) => Stream.empty
        }
  }
}

object StreamProducerExample extends IOApp {
  import StreamRate._

  def putStrLn[A](a: A): IO[Unit] = IO(println(a))

  def randomMessage: IO[Map[String, String]] = {
    val rndKey   = IO(Random.nextInt(1000).toString)
    val rndValue = IO(Random.nextString(10))
    (rndKey, rndValue).parMapN { case (k, v) =>
      Map(k -> v)
    }
  }

  def run(args: List[String]): IO[ExitCode] = {
    val r = for {
      // maxQueued: How many elements before new submissions semantically block. Tradeoff of memory to queue jobs. 
      // Default 10000 is good for small servers. But can easily take 100,000.
      // workers: How many threads will process pipelined messages.
      connection <- RedisConnection.queued[IO].withHost(host"localhost").withPort(port"6379").withMaxQueued(maxQueued = 10000).withWorkers(workers = 2).build
    } yield connection

    r
      .flatMap{ client => 
        Stream
          .awakeEvery[IO](10.micro)
          .evalMap(_ => randomMessage)
          .groupWithin(10000, 1.milli)
          .evalMap{ chunk => 
            // Send a Single Set of Pipelined Commands to the Redis Server
            val r = chunk.traverse(body => RedisCommands.xadd[RedisPipeline]("mystream", body))
            val multi = r.pipeline[IO]
            multi.run(client)
          }
          .unchunks
          .logAverageRate(rate => IO.println(s"Producer rate: $rate"))
          .compile
          .drain
          .background
      }
      .use(combined => IO.never)
      .redeem(
        { t =>
          IO.println(s"Error: $t, Something went wrong")
          ExitCode(1)
        },
        _ => ExitCode.Success
      )
  }
}