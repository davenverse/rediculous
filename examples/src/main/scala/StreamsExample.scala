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

  def randomMessage: IO[List[(String, String)]] = {
    val rndKey   = IO(Random.nextInt(1000).toString)
    val rndValue = IO(Random.nextString(500))
    (rndKey, rndValue).parMapN{ case (k, v) => List(k -> v) }
  }

  def run(args: List[String]): IO[ExitCode] = {
    val mystream = "mystream"

    RedisConnection.pool[IO].withHost(host"localhost").withPort(port"6379").build
      .map(conn=> (conn, RedisStream.fromConnection[IO](conn)))
      .use { case (conn, rs) => 
        val consumer = rs
          .read(Set(mystream))
          .flatMap(o => 
            Stream.emits(o.records.flatMap(s => s.keyValues))
          )
          .onError{ case err => Stream.exec(IO.println(s"Consumer err: $err"))}
          .logAverageRate(rate => IO.println(s"Consumer rate: $rate/s"))

        val producer = 
          Stream
            .repeatEval(randomMessage)
            .map(RedisStream.XAddMessage(mystream, _))
            .chunkMin(1000)
            .zipLeft(Stream.fixedDelay[IO](1.seconds))
            .flatMap{ chunk => 
              Stream.eval(rs.append(chunk))
                .unchunks
            }
            .onError{ case err => Stream.exec(IO.println(s"Producer err: $err"))}
            .logAverageRate(rate => IO.println(s"Producer rate: $rate/s"))

        val stream = 
          Stream.exec( RedisCommands.del[RedisPipeline]("mystream").pipeline[IO].run(conn).void) ++
            Stream.exec(IO.println("Started")) ++
            consumer
              .concurrently(producer)
              .interruptAfter(60.second)

          // Stream.eval( RedisCommands.xlen[RedisPipeline]("mystream").pipeline[IO].run(client).flatMap(length => IO.println(s"Finished: $length")))

        stream.compile.count.flatTap(l => IO.println(s"Consumer Length: $l"))
      }
      .redeem(
        { t =>
          IO.println(s"Error: $t, Something went wrong")
          ExitCode(1)
        },
        _ => ExitCode.Success
      )
  }
} 