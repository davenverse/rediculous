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
      // client <- sg.client[IO](new InetSocketAddress("localhost", 6379))
      connection <- Connection.queued[IO](sg, new InetSocketAddress("localhost", 6379))
    } yield connection

    r.use {client =>
      // val bytes = Resp.encode(
      
      // )
    
      List.fill(100)(Protocol.ping[IO]).sequence.run(client).use{ia => ia.sequence.flatMap(a => IO(println(a)))}
    } >>
      IO.pure(ExitCode.Success)
    
  }

}
import _root_.io.chrisdavenport.keypool.KeyPool

object Protocol {

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


    def queued[F[_]: Concurrent: Timer: ContextShift](sg: SocketGroup, address: InetSocketAddress): Resource[F, Connection[F]] = 
      for {
        queue <- Resource.liftF(Queue.bounded[F, (Deferred[F, Resp], Resp)](2000))
        keypool <- KeyPoolBuilder[F, Unit, (Socket[F], F[Unit])](
          {_ => sg.client[F](address).allocated.flatTap(a => Sync[F].delay(println(s"Created Redis Connection to $address")))},
          { case (_, shutdown) => Sync[F].delay(println("Shutting down redis connection")) >> shutdown}
        ).build
        _ <- 
            queue.dequeue.groupWithin(1000, 5.millis).map{chunk => 
              Stream.resource(
              chunk.toNel match {
                case Some(value) => 
                  keypool.map(_._1).take(()).evalMap{
                    m => explicitPipelineRequest(m.value, value.map(_._2)).attempt.flatTap{
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
            
            }.parJoin(2)
            .compile
            .drain
            .background
      } yield Queued(queue)
  }

  def ping[F[_]: Concurrent]: Kleisli[Resource[F, *], Connection[F], F[String]] = {
    val ping = Resp.Array(
      Some(
        List(
          Resp.BulkString(Some("PING")),
        )
      )
    )
  
    Kleisli{con: Connection[F] => Connection.run(con)(ping)}.map{_.flatMap{
      case Right(Resp.SimpleString(x)) => x.pure[F]
      case Right(Resp.BulkString(Some(x))) => x.pure[F]
      case Right(e@Resp.Error(_)) => ApplicativeError[F, Throwable].raiseError(e)
      case s => ApplicativeError[F, Throwable].raiseError(new Throwable("Incompatible Response Type for PING, got $s"))
    }}

  }

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