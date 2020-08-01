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
import _root_.io.chrisdavenport.rediculous.Protocol.Connection
object Main extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val r = for {
      blocker <- Blocker[IO]
      sg <- SocketGroup[IO](blocker)
      connection <- Connection.queued[IO](sg, new InetSocketAddress("localhost", 6379))
    } yield connection

    r.use {client =>
        val a = (
          Protocol.ping[IO],
          Protocol.get[IO]("foo"),
          Protocol.set[IO]("foo", "value"),
          Protocol.get[IO]("foo")
        ).parTupled

        a.run(client).flatTap(a => IO(println(a)))
      // val r = List.fill(25)(Protocol.ping[IO]).parSequence
      // (r.run(client).flatMap(a => IO(println(a)))
      // ,r.run(client).flatMap(a => IO(println(a)))
      // ).parMapN{ case (_, _) => ()}
    } >>
      IO.pure(ExitCode.Success)
    
  }

}
import _root_.io.chrisdavenport.keypool.KeyPool

object Protocol {

  final case class Redis[F[_], A](unRedis: Kleisli[Resource[F, *], Connection[F], F[A]]){
    def run(connection: Connection[F])(implicit ev: Bracket[F, Throwable]): F[A] = {
      Redis.runRedis(this)(connection)
    }
  }
  object Redis {

    private def runRedis[F[_]: Bracket[*[_], Throwable], A](redis: Redis[F, A])(connection: Connection[F]): F[A] = {
      redis.unRedis.run(connection).use{fa => fa}
    }

    def liftF[F[_]: Monad, A](fa: F[A]): Redis[F, A] = 
      Redis(Kleisli.liftF[Resource[F, *], Connection[F], A](Resource.liftF(fa)).map(_.pure[F]))
    def liftFBackground[F[_]: Concurrent, A](fa: F[A]): Redis[F, A] = Redis(
      Kleisli.liftF(fa.background)
    )

    /**
     * Newtype encoding for a `Redis` datatype that has a `cats.Applicative`
     * capable of doing parallel processing in `ap` and `map2`, needed
     * for implementing `cats.Parallel`.
     *
     * Helpers are provided for converting back and forth in `Par.apply`
     * for wrapping any `Redis` value and `Par.unwrap` for unwrapping.
     *
     * The encoding is based on the "newtypes" project by
     * Alexander Konovalov, chosen because it's devoid of boxing issues and
     * a good choice until opaque types will land in Scala.
     * [[https://github.com/alexknvl/newtypes alexknvl/newtypes]].
     *
     */
    type Par[+F[_], +A] = Par.Type[F, A]

    object Par {

      type Base
      trait Tag extends Any
      type Type[+F[_], +A] <: Base with Tag

      def apply[F[_], A](fa: Redis[F, A]): Type[F, A] =
        fa.asInstanceOf[Type[F, A]]

      def unwrap[F[_], A](fa: Type[F, A]): Redis[F, A] =
        fa.asInstanceOf[Redis[F, A]]

      def parallel[F[_]]: Redis[F, *] ~> Par[F, *] = new ~>[Redis[F, *], Par[F, *]]{
        def apply[A](fa: Redis[F,A]): Par[F,A] = Par(fa)
      }
      def sequential[F[_]]: Par[F, *] ~> Redis[F, *] = new ~>[Par[F, *], Redis[F, *]]{
        def apply[A](fa: Par[F,A]): Redis[F,A] = unwrap(fa)
      }

      implicit def parApplicative[F[_]: Parallel: Bracket[*[_], Throwable]]: Applicative[Par[F, *]] = new Applicative[Par[F, *]]{
        def ap[A, B](ff: Par[F,A => B])(fa: Par[F,A]): Par[F,B] = Par(Redis(
          Par.unwrap(ff).unRedis.flatMap{ ff => 
            Par.unwrap(fa).unRedis.map{fa =>  Parallel[F].sequential(
              Parallel[F].applicative.ap(Parallel[F].parallel(ff))(Parallel[F].parallel(fa))
            )}
          }
        ))
        def pure[A](x: A): Par[F,A] = Par(Redis(
          Kleisli.pure[Resource[F, *], Connection[F], F[A]](x.pure[F])
        ))
      }

    }

    implicit def monad[F[_]: Monad]: Monad[Redis[F, *]] = new StackSafeMonad[Redis[F, *]]{
      def flatMap[A, B](fa: Redis[F,A])(f: A => Redis[F,B]): Redis[F,B] = Redis(
        fa.unRedis.flatMap(fa => 
          Kleisli.liftF[Resource[F, *], Connection[F], A](Resource.liftF(fa))
            .flatMap(a => f(a).unRedis)
        )
      )
      def pure[A](x: A): Redis[F, A] = Redis(
        Kleisli.pure[Resource[F, *], Connection[F], F[A]](x.pure[F])
      )
    }

    

    implicit def parRedis[M[_]: Parallel: Concurrent]: Parallel[Redis[M, *]] = new Parallel[Redis[M, *]]{
      type F[A] = Par[M, A]

      def sequential: Par[M, *] ~> Redis[M, *] = Par.sequential[M]
      
      def parallel: Redis[M, *] ~> Par[M, *] = Par.parallel[M]
      
      def applicative: Applicative[Par[M, *]] = Par.parApplicative[M] 
      
      def monad: Monad[Redis[M,*]] = Redis.monad[M]
    }
  }

  trait RedisResult[+A]{
    def decode(resp: Resp): Either[Resp, A]
  }
  object RedisResult{
    def apply[A](implicit ev: RedisResult[A]): ev.type = ev
    implicit val resp: RedisResult[Resp] = new RedisResult[Resp]{
      def decode(resp: Resp): Either[Resp,Resp] = Right(resp)
    }
  }

  sealed trait Connection[F[_]]
  object Connection{
    case class Queued[F[_]](queue: Queue[F, (Deferred[F, Resp], Resp)]) extends Connection[F]
    case class PooledConnection[F[_]](
      pool: KeyPool[F, Unit, (Socket[F], F[Unit])]
    ) extends Connection[F]
    case class DirectConnection[F[_]](socket: Socket[F]) extends Connection[F]

    def run[F[_]: Concurrent, A: RedisResult](connection: Connection[F])(resp: Resp): Resource[F, F[Either[Resp, A]]] = {
      def withSocket(socket: Socket[F]): F[Resp] = explicitPipelineRequest[F](socket, NonEmptyList.of(resp)).map(_.head)
      connection match {
        case PooledConnection(pool) => pool.map(_._1).take(()).flatMap{
          m => withSocket(m.value).attempt.flatTap{
            case Left(e) => m.canBeReused.set(DontReuse)
            case _ => Applicative[F].unit
          }.rethrow.map(RedisResult[A].decode).background
        }
        case DirectConnection(socket) => withSocket(socket).map(RedisResult[A].decode).background
        case Queued(queue) => Resource.liftF(Deferred[F, Resp]).flatMap{d => 
          Resource.liftF(queue.enqueue1((d, resp))) >> d.get.map(RedisResult[A].decode).background      
        }
      }
    }

    def pool[F[_]: Concurrent: Timer: ContextShift](sg: SocketGroup, address: InetSocketAddress): Resource[F, Connection[F]] = 
      KeyPoolBuilder[F, Unit, (Socket[F], F[Unit])](
        {_ => sg.client[F](address).allocated.flatTap(a => Sync[F].delay(println(s"Created Redis Connection to $address")))},
        { case (_, shutdown) => Sync[F].delay(println("Shutting down redis connection")) >> shutdown}
      ).build.map(PooledConnection[F](_))


    def queued[F[_]: Concurrent: Timer: ContextShift](sg: SocketGroup, address: InetSocketAddress, maxQueued: Int = 1000, workers: Int = 2): Resource[F, Connection[F]] = 
      for {
        queue <- Resource.liftF(Queue.bounded[F, (Deferred[F, Resp], Resp)](maxQueued))
        keypool <- KeyPoolBuilder[F, Unit, (Socket[F], F[Unit])](
          {_ => sg.client[F](address).allocated.flatTap(a => Sync[F].delay(println(s"Created Redis Connection to $address")))},
          { case (_, shutdown) => Sync[F].delay(println("Shutting down redis connection")) >> shutdown}
        ).build
        _ <- 
            queue.dequeue.chunks.map{chunk => 
              Stream.resource(
              chunk.toNel match {
                case Some(value) => 
                  keypool.map(_._1).take(()).evalMap{m =>
                    val out = value.map(_._2)
                    Sync[F].delay(println(s"Sending Request for $out")) >>
                    explicitPipelineRequest(m.value, out).attempt.flatTap{// Currently Guarantee Chunk.size === returnSize
                      case Left(e) => m.canBeReused.set(DontReuse)
                      case _ => Applicative[F].unit
                    }.rethrow.flatMap{
                      n => n.zipWithIndex.traverse_{
                        case (ref, i) => 
                          val (toSet, _) = chunk(i)
                          toSet.complete(ref)
                      }
                    }
                  }
                case None => Resource.pure[F, Unit](())
              }
              )
            
            }.parJoin(workers) // Worker Threads
            .compile
            .drain
            .background
      } yield Queued(queue)
  }


  def ping[F[_]: Concurrent]: Redis[F, String] = {
    val ping = Resp.Array(
      Some(
        List(
          Resp.BulkString(Some("PING")),
        )
      )
    )
    Redis(
      Kleisli{con: Connection[F] => Connection.run(con)(ping)}.map{_.flatMap{
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
      Kleisli{con: Connection[F] => Connection.run(con)(ping)}.map{_.flatMap{
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
      Kleisli{con: Connection[F] => Connection.run(con)(ping)}.map{_.flatMap{
        case Right(Resp.SimpleString(x)) => x.some.pure[F]
        case Right(Resp.BulkString(Some(x))) => x.some.pure[F]
        case Right(Resp.BulkString(None)) => None.pure[F].widen
        case Right(e@Resp.Error(_)) => ApplicativeError[F, Throwable].raiseError(e)
        case s => ApplicativeError[F, Throwable].raiseError(new Throwable("Incompatible Response Type for PING, got $s"))
      }}
    )
  }


  // Guarantees With Socket That Each Call Receives a Response
  def explicitPipelineRequest[F[_]: MonadError[*[_], Throwable]](socket: Socket[F], calls: NonEmptyList[Resp], maxBytes: Int = 8 * 1024 * 1024, timeout: Option[FiniteDuration] = 5.seconds.some): F[NonEmptyList[Resp]] = {
    def getTillEqualSize(acc: List[List[Resp]], lastArr: Array[Byte]): F[NonEmptyList[Resp]] = 
    socket.read(maxBytes, timeout).flatMap{
      case None => 
        ApplicativeError[F, Throwable].raiseError[NonEmptyList[Resp]](new Throwable("Terminated Before reaching Equal size"))
      case Some(bytes) => 
        Resp.parseAll(lastArr ++ bytes.toArray) match {
          case e@Resp.ParseError(_, _) => ApplicativeError[F, Throwable].raiseError[NonEmptyList[Resp]](e)
          case Resp.ParseIncomplete(arr) => getTillEqualSize(acc, arr)
          case Resp.ParseComplete(value, rest) => 
            if (value.size + acc.foldMap(_.size) === calls.size) (value ::acc ).reverse.flatten.toNel.get.pure[F]
            else getTillEqualSize(value :: acc, rest)
          
        }
    }
    val arrayB = new scala.collection.mutable.ArrayBuffer[Byte]
      calls.toList.foreach{
        case resp => 
          arrayB.addAll(Resp.encode(resp))
      }
    socket.write(Chunk.bytes(arrayB.toArray)) >>
    getTillEqualSize(List.empty, Array.emptyByteArray)
  }

}