package io.chrisdavenport.rediculous


import cats.effect._
import cats.effect.concurrent._
import cats.effect.implicits._
import cats._
import cats.implicits._
import cats.data._
import io.chrisdavenport.keypool._
import fs2.concurrent.Queue
import fs2.io.tcp._
import fs2._
import java.net.InetSocketAddress
import scala.concurrent.duration._

sealed trait Connection[F[_]]
object Connection{
  private case class Queued[F[_]](queue: Queue[F, (Deferred[F, Resp], Resp)]) extends Connection[F]
  private case class PooledConnection[F[_]](
    pool: KeyPool[F, Unit, (Socket[F], F[Unit])]
  ) extends Connection[F]
  private case class DirectConnection[F[_]](socket: Socket[F]) extends Connection[F]

  // Guarantees With Socket That Each Call Receives a Response
  // Chunk must be non-empty but to do so incurs a penalty
  private[rediculous] def explicitPipelineRequest[F[_]: MonadError[*[_], Throwable]](socket: Socket[F], calls: Chunk[Resp], maxBytes: Int = 8 * 1024 * 1024, timeout: Option[FiniteDuration] = 5.seconds.some): F[List[Resp]] = {
    def getTillEqualSize(acc: List[List[Resp]], lastArr: Array[Byte]): F[List[Resp]] = 
    socket.read(maxBytes, timeout).flatMap{
      case None => 
        ApplicativeError[F, Throwable].raiseError[List[Resp]](new Throwable("Terminated Before reaching Equal size"))
      case Some(bytes) => 
        Resp.parseAll(lastArr ++ bytes.toArray) match {
          case e@Resp.ParseError(_, _) => ApplicativeError[F, Throwable].raiseError[List[Resp]](e)
          case Resp.ParseIncomplete(arr) => getTillEqualSize(acc, arr)
          case Resp.ParseComplete(value, rest) => 
            if (value.size + acc.foldMap(_.size) === calls.size) (value ::acc ).reverse.flatten.pure[F]
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

  // Can Be used to implement any low level protocols.
  def runRequest[F[_]: Concurrent, A: RedisResult](connection: Connection[F])(resp: Resp): Resource[F, F[Either[Resp, A]]] = {
    def withSocket(socket: Socket[F]): F[Resp] = explicitPipelineRequest[F](socket, Chunk.singleton(resp)).map(_.head)
    connection match {
      case PooledConnection(pool) => pool.map(_._1).take(()).evalMap{
        m => withSocket(m.value).attempt.flatTap{
          case Left(e) => m.canBeReused.set(Reusable.DontReuse)
          case _ => Applicative[F].unit
        }.rethrow.map(RedisResult[A].decode)
      }.map(_.pure[F])
      case DirectConnection(socket) => Resource.liftF(withSocket(socket).map(RedisResult[A].decode)).map(_.pure[F])
      case Queued(queue) => Resource.liftF(Deferred[F, Resp]).flatMap{d => 
        Resource.liftF(queue.enqueue1((d, resp))) >> {
          Resource.pure[F, F[Either[Resp, A]]](d.get.map(RedisResult[A].decode))
        }     
      }
    }
  }

  def single[F[_]: Concurrent: ContextShift](sg: SocketGroup, address: InetSocketAddress): Resource[F, Connection[F]] = 
    sg.client[F](address).map(Connection.DirectConnection(_))

  def pool[F[_]: Concurrent: Timer: ContextShift](sg: SocketGroup, address: InetSocketAddress): Resource[F, Connection[F]] = 
    KeyPoolBuilder[F, Unit, (Socket[F], F[Unit])](
      {_ => sg.client[F](address).allocated.flatTap(a => Sync[F].delay(println(s"Created Redis Connection to $address")))},
      { case (_, shutdown) => Sync[F].delay(println("Shutting down redis connection")) >> shutdown}
    ).build.map(PooledConnection[F](_))

  // Only allows 1k queued actions, before new actions block to be accepted.
  def queued[F[_]: Concurrent: Timer: ContextShift](sg: SocketGroup, address: InetSocketAddress, maxQueued: Int = 1000, workers: Int = 2): Resource[F, Connection[F]] = 
    for {
      queue <- Resource.liftF(Queue.bounded[F, (Deferred[F, Resp], Resp)](maxQueued))
      keypool <- KeyPoolBuilder[F, Unit, (Socket[F], F[Unit])](
        {_ => sg.client[F](address).allocated.flatTap(a => Sync[F].delay(println(s"Created Redis Connection to $address")))},
        { case (_, shutdown) => Sync[F].delay(println("Shutting down redis connection")) >> shutdown}
      ).build
      _ <- 
          queue.dequeue.chunks.map{chunk => 
            if (chunk.nonEmpty) {
                Stream.eval(keypool.map(_._1).take(()).use{m =>
                  val out = chunk.map(_._2)
                  // Sync[F].delay(println(s"Sending Request for $out")) >>
                  explicitPipelineRequest(m.value, out).attempt.flatTap{// Currently Guarantee Chunk.size === returnSize
                    case Left(e) => m.canBeReused.set(Reusable.DontReuse)
                    case _ => Applicative[F].unit
                  }.rethrow
                }.flatMap{
                    n => n.zipWithIndex.traverse_{
                      case (ref, i) => 
                        val (toSet, _) = chunk(i)
                        toSet.complete(ref)
                    }
                })
            } else {
              Stream.empty
            }
          
          }.parJoin(workers) // Worker Threads
          .compile
          .drain
          .background
    } yield Queued(queue)
}