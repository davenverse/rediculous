// import io.chrisdavenport.rediculous._
// import cats.implicits._
// import cats.data._
// import cats.effect._
// import fs2.io.net._
// import com.comcast.ip4s._
// import io.chrisdavenport.rediculous.cluster.ClusterCommands
// import scala.concurrent.duration._

// /**
//  * Test App For Development Purposes
//  **/
// object ATestApp extends IOApp {
// import io.chrisdavenport.rediculous.Resp._
//   def run(args: List[String]): IO[ExitCode] = {
//     // RedisConnection.queued[IO].withHost(host"localhost").withPort(port"6379").withMaxQueued(10000).withWorkers(workers = 1).build.flatMap{
//     //   connection => 
//     //     RedisPubSub.fromConnection(connection, 4096).map(alg => (connection, alg))
//     // }.use{ case(conn, alg) => 
//     //     alg.nonMessages({r => IO.println(s"other: $r")}) >>
//     //     alg.unhandledMessages({r => IO.println(s"unhandled: $r")}) >>
//     //     alg.psubscribe(all, {r => IO.println("p: " + r.toString())}) >>
//     //     alg.subscribe(foo, {r  => IO.println("s: " + r.toString())}) >> {
//     //       (
//     //         alg.runMessages,
//     //         alg.publish(foo, "Baz"),
//     //         Temporal[IO].sleep(10.seconds) >> 
//     //         alg.subscriptions.flatTap(IO.println(_)) >> 
//     //         alg.psubscriptions.flatTap(IO.println(_))
//     //       ).parMapN{ case (_, _, _) => ()} 
//     //     }
//     // val resp = Array(Some(
//     //     List(
//     //       Array(Some(
//     //           List(
//     //             BulkString(Some("mystream")),
//     //             Array(Some(
//     //                 List(
//     //                   Array(Some(
//     //                       List(
//     //                         BulkString(Some("1639792169819-0")),
//     //                         Array(Some(
//     //                           List(
//     //                             BulkString(Some("sensor-id")), BulkString(Some("1234")),
//     //                             BulkString(Some("temperature")), BulkString(Some("12")),
//     //                             BulkString(Some("ba")), BulkString(Some("123"))
//     //                           )
//     //                         ))
//     //                       )
//     //                   ))
//     //                 )
//     //             ))
//     //           )
//     //       ))
//     //   )
//     // ))
//     IO.println(Resp.toStringRedisCLI(resp))
//     }.as(ExitCode.Success)
//     // val r = for {
//     //   connection <- RedisConnection.pool[IO].withHost(host"localhost").withPort(port"30001").build
//     // } yield connection

//     // r.use {con =>
//     //   RedisConnection.runRequestTotal[IO, ClusterCommands.ClusterSlots](NonEmptyList.of("CLUSTER", "SLOTS"), None).unRedis.run(con)
//     //   .flatTap(r => IO(println(r)))
//     // } >>
//     //   IO.pure(ExitCode.Success)
    

// }