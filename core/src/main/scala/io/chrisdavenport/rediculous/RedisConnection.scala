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
import _root_.io.chrisdavenport.rediculous.cluster.HashSlot
import _root_.io.chrisdavenport.rediculous.cluster.ClusterCommands
import fs2.io.tls.TLSContext
import fs2.io.tls.TLSParameters

sealed trait RedisConnection[F[_]]
object RedisConnection{
  private case class Queued[F[_]](queue: Queue[F, Chunk[(Deferred[F, Either[Throwable, Resp]], Resp)]], usePool: Resource[F, Managed[F, Socket[F]]]) extends RedisConnection[F]
  private case class PooledConnection[F[_]](
    pool: KeyPool[F, Unit, (Socket[F], F[Unit])]
  ) extends RedisConnection[F]
  private case class DirectConnection[F[_]](socket: Socket[F]) extends RedisConnection[F]

  private case class Cluster[F[_]](queue: Queue[F, Chunk[(Deferred[F, Either[Throwable, Resp]], RedisCtx.CtxType, Option[(String, Int)], Int, Resp)]]) extends RedisConnection[F]

  // Guarantees With Socket That Each Call Receives a Response
  // Chunk must be non-empty but to do so incurs a penalty
  private[rediculous] def explicitPipelineRequest[F[_]: MonadError[*[_], Throwable]](socket: Socket[F], calls: Chunk[Resp], maxBytes: Int = 8 * 1024 * 1024, timeout: Option[FiniteDuration] = 5.seconds.some): F[List[Resp]] = {
    def getTillEqualSize(acc: List[List[Resp]], lastArr: Array[Byte]): F[List[Resp]] = 
    socket.read(maxBytes, timeout).flatMap{
      case None => 
        ApplicativeError[F, Throwable].raiseError[List[Resp]](RedisError.Generic("Rediculous: Terminated Before reaching Equal size"))
      case Some(bytes) => 
        Resp.parseAll(lastArr.toArray ++ bytes.toArray.toIterable) match {
          case e@Resp.ParseError(_, _) => ApplicativeError[F, Throwable].raiseError[List[Resp]](e)
          case Resp.ParseIncomplete(arr) => getTillEqualSize(acc, arr)
          case Resp.ParseComplete(value, rest) => 
            if (value.size + acc.foldMap(_.size) === calls.size) (value ::acc ).reverse.flatten.pure[F]
            else getTillEqualSize(value :: acc, rest)
          
        }
    }
    if (calls.nonEmpty){
      val arrayB = new scala.collection.mutable.ArrayBuffer[Byte]
        calls.toList.foreach{
          case resp => 
            arrayB.++=(Resp.encode(resp))
        }
      socket.write(Chunk.bytes(arrayB.toArray)) >>
      getTillEqualSize(List.empty, Array.emptyByteArray)
    } else Applicative[F].pure(List.empty)
  }

  def runRequestInternal[F[_]: Concurrent](connection: RedisConnection[F])(
    inputs: NonEmptyList[NonEmptyList[String]],
    ctx: RedisCtx.CtxType
  ): F[F[NonEmptyList[Resp]]] = {
      val chunk = Chunk.seq(inputs.toList.map(Resp.renderRequest))
      def withSocket(socket: Socket[F]): F[NonEmptyList[Resp]] = explicitPipelineRequest[F](socket, chunk).flatMap(l => Sync[F].delay(l.toNel.getOrElse(throw RedisError.Generic("Rediculous: Impossible Return List was Empty but we guarantee output matches input"))))
      connection match {
      case PooledConnection(pool) => pool.map(_._1).take(()).use{
        m => withSocket(m.value).attempt.flatTap{
          case Left(_) => m.canBeReused.set(Reusable.DontReuse)
          case _ => Applicative[F].unit
        }
      }.rethrow.map(_.pure[F])
      case DirectConnection(socket) => withSocket(socket).map(_.pure[F])
      case Queued(queue, _) => chunk.traverse(resp => Deferred[F, Either[Throwable, Resp]].map((_, resp))).flatMap{ c => 
        queue.enqueue1(c).as {
          c.traverse(_._1.get).flatMap(_.sequence.traverse(l => Sync[F].delay(l.toNel.getOrElse(throw RedisError.Generic("Rediculous: Impossible Return List was Empty but we guarantee output matches input"))))).rethrow
        }   
      }
      case Cluster(queue) => chunk.traverse(resp => Deferred[F, Either[Throwable, Resp]].map((_, ctx, None, 0, resp))).flatMap{ c => 
        queue.enqueue1(c).as {
          c.traverse(_._1.get).flatMap(_.sequence.traverse(l => Sync[F].delay(l.toNel.getOrElse(throw RedisError.Generic("Rediculous: Impossible Return List was Empty but we guarantee output matches input"))))).rethrow
        }
      }   
    }
  }

  // Can Be used to implement any low level protocols.
  def runRequest[F[_]: Concurrent, A: RedisResult](connection: RedisConnection[F])(input: NonEmptyList[String], ctx: RedisCtx.CtxType): F[F[Either[Resp, A]]] = 
    runRequestInternal(connection)(NonEmptyList.of(input), ctx).map(_.map(nel => RedisResult[A].decode(nel.head)))

  def runRequestTotal[F[_]: Concurrent, A: RedisResult](input: NonEmptyList[String], ctx: RedisCtx.CtxType): Redis[F, A] = Redis(Kleisli{connection: RedisConnection[F] => 
    runRequest(connection)(input, ctx).map{ fE => 
      fE.flatMap{
        case Right(a) => a.pure[F]
        case Left(e@Resp.Error(_)) => ApplicativeError[F, Throwable].raiseError[A](e)
        case Left(other) => ApplicativeError[F, Throwable].raiseError[A](RedisError.Generic(s"Rediculous: Incompatible Return Type for Operation: ${input.head}, got: $other"))
      }
    }
  })

  private[rediculous] def closeReturn[F[_]: MonadError[*[_], Throwable], A](fE: F[Either[Resp, A]]): F[A] = 
    fE.flatMap{
        case Right(a) => a.pure[F]
        case Left(e@Resp.Error(_)) => ApplicativeError[F, Throwable].raiseError[A](e)
        case Left(other) => ApplicativeError[F, Throwable].raiseError[A](RedisError.Generic(s"Rediculous: Incompatible Return Type: Got $other"))
      }

  def single[F[_]: Concurrent: ContextShift](
    sg: SocketGroup,
    host: String,
    port: Int,
    tlsContext: Option[TLSContext] = None,
    tlsParameters: TLSParameters = TLSParameters.Default
  ): Resource[F, RedisConnection[F]] = 
    for {
      socket <- sg.client[F](new InetSocketAddress(host, port))
      out <- elevateSocket(socket, tlsContext, tlsParameters)
    } yield RedisConnection.DirectConnection(out)

  def pool[F[_]: Concurrent: Timer: ContextShift](
    sg: SocketGroup,
    host: String,
    port: Int,
    tlsContext: Option[TLSContext] = None,
    tlsParameters: TLSParameters = TLSParameters.Default
  ): Resource[F, RedisConnection[F]] = 
    KeyPoolBuilder[F, Unit, (Socket[F], F[Unit])](
      {_ => sg.client[F](new InetSocketAddress(host, port)).flatMap(elevateSocket(_, tlsContext, tlsParameters)).allocated},
      { case (_, shutdown) => shutdown}
    ).build.map(PooledConnection[F](_))

  // Only allows 1k queued actions, before new actions block to be accepted.
  def queued[F[_]: Concurrent: Timer: ContextShift](
    sg: SocketGroup,
    host: String,
    port: Int,
    maxQueued: Int = 10000,
    workers: Int = 2,
    tlsContext: Option[TLSContext] = None,
    tlsParameters: TLSParameters = TLSParameters.Default
  ): Resource[F, RedisConnection[F]] = 
    for {
      queue <- Resource.liftF(Queue.bounded[F, Chunk[(Deferred[F, Either[Throwable,Resp]], Resp)]](maxQueued))
      keypool <- KeyPoolBuilder[F, Unit, (Socket[F], F[Unit])](
        {_ => sg.client[F](new InetSocketAddress(host, port))
          .flatMap(elevateSocket(_, tlsContext, tlsParameters))
          .allocated
        },
        { case (_, shutdown) => shutdown}
      ).build
      _ <- 
          queue.dequeue.chunks.map{chunkChunk =>
            val chunk = chunkChunk.flatten
            val s = if (chunk.nonEmpty) {
                Stream.eval(keypool.map(_._1).take(()).use{m =>
                  val out = chunk.map(_._2)
                  explicitPipelineRequest(m.value, out).attempt.flatTap{// Currently Guarantee Chunk.size === returnSize
                    case Left(_) => m.canBeReused.set(Reusable.DontReuse)
                    case _ => Applicative[F].unit
                  }
                }.flatMap{
                  case Right(n) => 
                    n.zipWithIndex.traverse_{
                      case (ref, i) => 
                        val (toSet, _) = chunk(i)
                        toSet.complete(Either.right(ref))
                    }
                  case e@Left(_) => 
                    chunk.traverse_{ case (deff, _) => deff.complete(e.asInstanceOf[Either[Throwable, Resp]])}
                }) 
            } else {
              Stream.empty
            }
            s ++ Stream.eval_(ContextShift[F].shift)
          }.parJoin(workers) // Worker Threads
          .compile
          .drain
          .background
    } yield Queued(queue, keypool.take(()).map(_.map(_._1)))

  def cluster[F[_]: Concurrent: Parallel: Timer: ContextShift](
    sg: SocketGroup,
    host: String,
    port: Int,
    maxQueued: Int = 10000,
    workers: Int = 2,
    parallelServerCalls: Int = Int.MaxValue,
    tlsContext: Option[TLSContext] = None,
    tlsParameters: TLSParameters = TLSParameters.Default
  ): Resource[F, RedisConnection[F]] = 
    for {
      keypool <- KeyPoolBuilder[F, (String, Int), (Socket[F], F[Unit])](
        {t: (String, Int) => sg.client[F](new InetSocketAddress(t._1, t._2))
            .flatMap(elevateSocket(_, tlsContext, tlsParameters))
            .allocated
        },
        { case (_, shutdown) => shutdown}
      ).build
      sockets <- Resource.liftF(keypool.take((host, port)).map(_.value._1).map(DirectConnection(_)).use(ClusterCommands.clusterslots[Redis[F, *]].run(_)))
      refTopology <- Resource.liftF(Ref[F].of(sockets))
      queue <- Resource.liftF(Queue.bounded[F, Chunk[(Deferred[F, Either[Throwable,Resp]], RedisCtx.CtxType, Option[(String, Int)], Int, Resp)]](maxQueued))
      cluster = Cluster(queue)
      refreshTopology = refTopology.get.flatMap(_.random).flatMap{ case (host, port) =>
        keypool.take((host, port)).map(_.value._1).map(DirectConnection(_)).use(ClusterCommands.clusterslots[Redis[F, *]].run(_))
      }.flatMap(s => refTopology.set(s))
      _ <- 
          queue.dequeue.chunks.map{chunkChunk =>
            val chunk = chunkChunk.flatten
            val s = if (chunk.nonEmpty) {
              Stream.eval(refTopology.get).map{topo => 
                Stream.eval(topo.random[F]).flatMap{ default => 
                Stream.emits(
                  sortToServers(chunk, {s => topo.served(HashSlot.find(s))}, default, topo.all).toList
                ).evalMap{
                  case (server, rest) => 
                    keypool.map(_._1).take(server).use{m =>
                      val out = Chunk.seq(rest.map(_._5).toList)
                      explicitPipelineRequest(m.value, out).attempt.flatTap{// Currently Guarantee Chunk.size === returnSize
                        case Left(_) => m.canBeReused.set(Reusable.DontReuse)
                        case _ => Applicative[F].unit
                      }
                    }.flatMap{
                    case Right(n) => 
                      n.zipWithIndex.traverse_[F, Unit]{
                        case (ref, i) => 
                          val (toSet, key, _, retries, initialCommand) = rest.toList(i)
                          ref match {
                            case e@Resp.Error(s) if (s.startsWith("MOVED") && retries <= 5)  => // MOVED 1234-2020 127.0.0.1:6381
                              refreshTopology.attempt.void >>
                              // Offer To Have it reprocessed. 
                              // If the queue is full return the error to the user
                              cluster.queue.offer1(Chunk.singleton((toSet, key, extractServer(s), retries + 1, initialCommand)))
                                .ifM( 
                                  Applicative[F].unit,
                                  toSet.complete(Either.right(e))
                                )
                            case e@Resp.Error(s) if (s.startsWith("ASK") && retries <= 5) => // ASK 1234-2020 127.0.0.1:6381
                              val serverRedirect = extractServer(s)
                              serverRedirect match {
                                case s@Some(_) => // This is a Special One Off, Requires a Redirect
                                  Deferred[F, Either[Throwable, Resp]].flatMap{d => // No One Cares About this Callback
                                    val asking = (d, key, s, 6, Resp.renderRequest(NonEmptyList.of("ASKING"))) // Never Repeat Asking
                                    val repeat = (toSet, key, s, retries + 1, initialCommand)
                                    val chunk = Chunk(asking, repeat)
                                    cluster.queue.offer1(chunk) // Offer To Have it reprocessed. 
                                      //If the queue is full return the error to the user
                                      .ifM(
                                        Applicative[F].unit,
                                        toSet.complete(Either.right(e))
                                      )
                                  }
                                case None => 
                                  toSet.complete(Either.right(e))
                              }
                            case otherwise => 
                              toSet.complete(Either.right(otherwise))
                          }
                      }
                    case e@Left(_) => 
                      rest.traverse_{ case (deff, _, _, _, _) => deff.complete(e.asInstanceOf[Either[Throwable, Resp]])}
                  }

                }
              }}.parJoin(parallelServerCalls) // Send All Acquired values simultaneously. Should be mostly IO awaiting callback
            } else Stream.empty
            s ++ Stream.eval_(ContextShift[F].shift)
          }.parJoin(workers)
            .compile
            .drain
            .background
    } yield cluster

  private def elevateSocket[F[_]: Concurrent: ContextShift](socket: Socket[F], tlsContext: Option[TLSContext], tlsParameters: TLSParameters): Resource[F, Socket[F]] = 
    tlsContext.fold(Resource.pure[F, Socket[F]](socket))(c => c.client(socket, tlsParameters))

  // ASK 1234-2020 127.0.0.1:6381
  // MOVED 1234-2020 127.0.0.1:6381
  private def extractServer(s: String): Option[(String, Int)] = {
    val end = s.lastIndexOf(' ')
    val portSplit = s.lastIndexOf(':')
    if (end > 0 &&  portSplit >= end + 1){
      val host = s.substring(end + 1, portSplit)
      val port = s.substring(portSplit +1, s.length())
      Either.catchNonFatal(port.toInt).toOption.map((host, _))
    } else None
  }

  private def sortToServers[F[_]](
    chunk: Chunk[(Deferred[F, Either[Throwable, Resp]], RedisCtx.CtxType, Option[(String, Int)], Int, Resp)],
    f: String => Option[(String, Int)],
    default: (String, Int),
    all: Set[(String, Int)]
  ): Map[(String, Int), NonEmptyList[(Deferred[F, Either[Throwable, Resp]], RedisCtx.CtxType, Option[(String, Int)], Int, Resp)]] = {
    groupByMany(chunk){ case (_, ctx, server, _, _) => 
      server.map(Set(_)).getOrElse(
        ctx match {
          case RedisCtx.CtxType.Keyed(key) => f(key).map(Set(_)).getOrElse(Set(default))
          case RedisCtx.CtxType.Random => Set(default)
          case RedisCtx.CtxType.Broadcast => all
        }
      )
    }
  }

  private def groupByMany[F[_], A](fa: F[A]): GroupByMany[F, A]= new GroupByMany(fa) 
    
  private class GroupByMany[F[_], A](fa: F[A]){
    def apply[K](f: A => Set[K])(implicit F: Foldable[F]): Map[K, NonEmptyList[A]] = {
      Foldable[F].foldRight(fa, Eval.now(Map.empty[K, NonEmptyList[A]])){
        case (x, eva) => eva.map{ acc => 
          val k = f(x)
          k.foldLeft(acc){ case (acc, k) =>
            val nextAcc = acc.get(k).fold(
              acc + (k -> NonEmptyList.of(x))
            )( value => 
              acc + (k -> value.prepend(x))
            )
            nextAcc
          }
        }
      }.value
    }
  }
}