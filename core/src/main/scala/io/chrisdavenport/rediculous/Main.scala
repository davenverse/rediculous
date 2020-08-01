package io.chrisdavenport.rediculous

import cats._
import cats.implicits._
import cats.effect._
import cats.effect.implicits._
import cats.data._
import cats.effect.concurrent._
import fs2.io.tcp._
import fs2.concurrent.Queue
import java.net.InetSocketAddress
import fs2._
import scala.concurrent.duration._
import _root_.io.chrisdavenport.keypool.KeyPoolBuilder
import java.net.InetAddress
import _root_.io.chrisdavenport.keypool.Reusable.DontReuse

object Main extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val r = for {
      blocker <- Blocker[IO]
      sg <- SocketGroup[IO](blocker)
      connection <- Connection.queued[IO](sg, new InetSocketAddress("localhost", 6379), workers = 4)
    } yield connection

    r.use {client =>
        val r = (
          Protocol.ping[IO],
          Protocol.get[IO]("foo"),
          Protocol.set[IO]("foo", "value"),
          Protocol.get[IO]("foo")
        ).parTupled

        // a.run(client).flatTap(a => IO(println(a)))
      // val r = List.fill(100)(Protocol.ping[IO]).parSequence
      Stream(Stream.eval(r.run(client)).repeat).parJoin(50).take(1000000).compile.drain
      // (r.run(client).flatMap(a => IO(println(a)))
      // ,r.run(client).flatMap(a => IO(println(a)))
      // ).parMapN{ case (_, _) => ()}
    } >>
      IO.pure(ExitCode.Success)
    
  }

}

object Protocol {

  def ping[F[_]: Concurrent]: Redis[F, String] = {
    val ping = Resp.Array(
      Some(
        List(
          Resp.BulkString(Some("PING")),
        )
      )
    )
    Redis(
      Kleisli{con: Connection[F] => Connection.runRequest(con)(ping)}.map{_.flatMap{
        case Right(Resp.SimpleString(x)) => x.pure[F]
        case Right(Resp.BulkString(Some(x))) => x.pure[F]
        case Right(e@Resp.Error(_)) => ApplicativeError[F, Throwable].raiseError(e)
        case s => ApplicativeError[F, Throwable].raiseError(new Throwable("Incompatible Response Type for PING, got $s"))
      }}
    )
  }

  def get[F[_]: Concurrent](key: String): Redis[F, Option[String]] = {
    val ping = Resp.Array(
      Some(
        List(
          Resp.BulkString(Some("GET")),
          Resp.BulkString(Some(key))
        )
      )
    )
    Redis(
      Kleisli{con: Connection[F] => Connection.runRequest(con)(ping)}.map{_.flatMap{
        case Right(Resp.SimpleString(x)) => x.some.pure[F]
        case Right(Resp.BulkString(Some(x))) => x.some.pure[F]
        case Right(Resp.BulkString(None)) => None.pure[F].widen
        case Right(e@Resp.Error(_)) => ApplicativeError[F, Throwable].raiseError(e)
        case s => ApplicativeError[F, Throwable].raiseError(new Throwable("Incompatible Response Type for PING, got $s"))
      }}
    )
  }

  def set[F[_]: Concurrent](key: String, value: String): Redis[F, Option[String]] = {
    val ping = Resp.Array(
      Some(
        List(
          Resp.BulkString(Some("SET")),
          Resp.BulkString(Some(key)),
          Resp.BulkString(Some(value))
        )
      )
    )
    Redis(
      Kleisli{con: Connection[F] => Connection.runRequest(con)(ping)}.map{_.flatMap{
        case Right(Resp.SimpleString(x)) => x.some.pure[F]
        case Right(Resp.BulkString(Some(x))) => x.some.pure[F]
        case Right(Resp.BulkString(None)) => None.pure[F].widen
        case Right(e@Resp.Error(_)) => ApplicativeError[F, Throwable].raiseError(e)
        case s => ApplicativeError[F, Throwable].raiseError(new Throwable("Incompatible Response Type for PING, got $s"))
      }}
    )
  }

}