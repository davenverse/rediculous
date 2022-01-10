package io.chrisdavenport.rediculous

import cats.effect._
import cats.effect.implicits._
import cats._
import cats.implicits._
import cats.data._
import _root_.org.typelevel.keypool._
import cats.effect.std._
import fs2.io.net._
import fs2._
import scala.concurrent.duration._
import com.comcast.ip4s._
import _root_.io.chrisdavenport.rediculous.cluster.HashSlot
import _root_.io.chrisdavenport.rediculous.cluster.ClusterCommands
import fs2.io.net.tls.TLSContext
import fs2.io.net.tls.TLSParameters
import java.time.Instant
import _root_.io.chrisdavenport.rediculous.cluster.ClusterCommands.ClusterSlots
import fs2.io.net.SocketGroupCompanionPlatform
import util.BufferedSocket
import scodec.bits.ByteVector

sealed trait RedisConnection[F[_]]
object RedisConnection{
  
  private[rediculous] case class Queued[F[_]](queue: Queue[F, Chunk[(Either[Throwable, Resp] => F[Unit], Resp)]], usePool: Resource[F, Managed[F, BufferedSocket[F]]]) extends RedisConnection[F]
  private[rediculous] case class PooledConnection[F[_]](
    pool: KeyPool[F, Unit, (BufferedSocket[F], F[Unit])]
  ) extends RedisConnection[F]

  private[rediculous] case class DirectConnection[F[_]](socket: BufferedSocket[F]) extends RedisConnection[F]

  private[rediculous] case class Cluster[F[_]](queue: Queue[F, Chunk[(Either[Throwable, Resp] => F[Unit], Option[String], Option[(Host, Port)], Int, Resp)]], slots: F[ClusterSlots], usePool: (Host, Port) => Resource[F, Managed[F, BufferedSocket[F]]]) extends RedisConnection[F]

  // Guarantees With Socket That Each Call Receives a Response
  // Chunk must be non-empty but to do so incurs a penalty
  private[rediculous] def explicitPipelineRequest[F[_]: Async](socket: BufferedSocket[F], calls: Chunk[Resp], maxBytes: Int = 8 * 1024 * 1024, timeout: Option[FiniteDuration] = 5.seconds.some): F[Chunk[Resp]] = {
    val out = calls.flatMap(resp => 
      Resp.CodecUtils.codec.encode(resp).toEither.traverse(bits => Chunk.byteVector(bits.bytes))
    ).sequence.leftMap(err => new Throwable(s"Failed To Encode Response $err")).liftTo[F]
    out.flatMap(socket.write) >> 
    socket.reads.through(fs2.interop.scodec.StreamDecoder.many(Resp.CodecUtils.codec).toPipeByte)
      .take(calls.size)
      .compile
      .to(Chunk)
  }

  def runRequestInternal[F[_]: Async](connection: RedisConnection[F])(
    inputs: Chunk[NonEmptyList[String]],
    key: Option[String]
  ): F[Chunk[Resp]] = {
      val chunk = Chunk.seq(inputs.toList.map(Resp.renderRequest))
      def withSocket(socket: BufferedSocket[F]): F[Chunk[Resp]] = explicitPipelineRequest[F](socket, chunk)

      connection match {
      case PooledConnection(pool) => Functor[({type M[A] = KeyPool[F, Unit, A]})#M].map(pool)(_._1).take(()).use{
        m => withSocket(m.value).attempt.flatTap{
          case Left(_) => m.canBeReused.set(Reusable.DontReuse)
          case _ => Applicative[F].unit
        }
      }.rethrow
      case DirectConnection(socket) => withSocket(socket)
      case Queued(queue, _) => chunk.traverse(resp => Deferred[F, Either[Throwable, Resp]].map(d => (d, ({(e: Either[Throwable, Resp]) => d.complete(e).void}, resp)))).flatMap{ c => 
        queue.offer(c.map(_._2)) >> {
          val x: F[Chunk[Either[Throwable, Resp]]] = c.traverse{ case (d, _) => d.get }
          val y: F[Chunk[Resp]] = x.flatMap(_.sequence.liftTo[F])
          y
        }   
      }
      case Cluster(queue, _, _) => chunk.traverse(resp => Deferred[F, Either[Throwable, Resp]].map(d => (d, ({(e: Either[Throwable, Resp]) => d.complete(e).void}, key, None, 0, resp)))).flatMap{ c => 
        queue.offer(c.map(_._2)) >> {
          c.traverse(_._1.get).flatMap(_.sequence.liftTo[F])
        }
      }   
    }
  }

  def toNel[F[_]: ApplicativeThrow](chunk: Chunk[Resp]): F[NonEmptyList[Resp]] = 
    chunk.toNel.liftTo[F](RedisError.Generic("Rediculous: Impossible Return List was Empty but we guarantee output matches input"))

  def head[F[_]: ApplicativeThrow](chunk: Chunk[Resp]): F[Resp] = 
    chunk.head.liftTo[F](RedisError.Generic("Rediculous: Impossible Return List was Empty but we guarantee output matches input"))

  // Can Be used to implement any low level protocols.
  def runRequest[F[_]: Async, A: RedisResult](connection: RedisConnection[F])(input: NonEmptyList[String], key: Option[String]): F[Either[Resp, A]] = 
    runRequestInternal(connection)(Chunk.singleton(input), key).flatMap(head[F]).map(resp => RedisResult[A].decode(resp))

  def runRequestTotal[F[_]: Async, A: RedisResult](input: NonEmptyList[String], key: Option[String]): Redis[F, A] = Redis(Kleisli{(connection: RedisConnection[F]) => 
    runRequest(connection)(input, key).flatMap{
      case Right(a) => a.pure[F]
      case Left(e@Resp.Error(_)) => ApplicativeError[F, Throwable].raiseError[A](e)
      case Left(other) => ApplicativeError[F, Throwable].raiseError[A](RedisError.Generic(s"Rediculous: Incompatible Return Type for Operation: ${input.head}, got:\n${Resp.toStringRedisCLI(other)}"))
    }
  })

  private[rediculous] def closeReturn[F[_]: MonadThrow, A](fE: Either[Resp, A]): F[A] = 
    fE match {
        case Right(a) => a.pure[F]
        case Left(e@Resp.Error(_)) => ApplicativeError[F, Throwable].raiseError[A](e)
        case Left(other) => ApplicativeError[F, Throwable].raiseError[A](RedisError.Generic(s"Rediculous: Incompatible Return Type, got\n${Resp.toStringRedisCLI(other)}"))
      }

  object Defaults {
    val host = host"localhost"
    val port = port"6379"
    val maxQueued = 10000
    val workers = 2
    val chunkSizeLimit = 256
    val clusterParallelServerCalls: Int = Int.MaxValue // Number of calls for cluster to execute in parallel against multiple server in a batch
    val clusterUseDynamicRefreshSource: Boolean = true // Set to false to only use initially provided host for topology refresh
    val clusterCacheTopologySeconds: FiniteDuration = 1.second // How long topology will not be rechecked for after a succesful refresh
  }

  def direct[F[_]: Async]: DirectConnectionBuilder[F] = 
    new DirectConnectionBuilder(
      Network[F],
      Defaults.host,
      Defaults.port,
      None,
      TLSParameters.Default
    )

  class DirectConnectionBuilder[F[_]: Async] private[RedisConnection](
    private val sg: SocketGroup[F],
    private val host: Host,
    private val port: Port,
    private val tlsContext: Option[TLSContext[F]],
    private val tlsParameters: TLSParameters 
  ) { self => 

    private def copy(
      sg: SocketGroup[F] = self.sg,
      host: Host = self.host,
      port: Port = self.port,
      tlsContext: Option[TLSContext[F]] = self.tlsContext,
      tlsParameters: TLSParameters = self.tlsParameters
    ): DirectConnectionBuilder[F] = new DirectConnectionBuilder(
      sg,
      host,
      port,
      tlsContext,
      tlsParameters
    )

    def withHost(host: Host) = copy(host = host)
    def withPort(port: Port) = copy(port = port)
    def withTLSContext(tlsContext: TLSContext[F]) = copy(tlsContext = tlsContext.some)
    def withoutTLSContext = copy(tlsContext = None)
    def withTLSParameters(tlsParameters: TLSParameters) = copy(tlsParameters = tlsParameters)
    def withSocketGroup(sg: SocketGroup[F]) = copy(sg = sg)

    def build: Resource[F,RedisConnection[F]] = 
      for {
        socket <- sg.client(SocketAddress(host,port), Nil)
        out <- elevateSocket(socket, tlsContext, tlsParameters)
        buffered <- Resource.eval(BufferedSocket.fromSocket[F](out))
      } yield RedisConnection.DirectConnection(buffered)
  }

  def pool[F[_]: Async]: PooledConnectionBuilder[F] = 
    new PooledConnectionBuilder(
      Network[F],
      Defaults.host,
      Defaults.port,
      None,
      TLSParameters.Default
    )

  class PooledConnectionBuilder[F[_]: Async] private[RedisConnection] (
    private val sg: SocketGroup[F],
    private val host: Host,
    private val port: Port,
    private val tlsContext: Option[TLSContext[F]],
    private val tlsParameters: TLSParameters 
  ) { self => 

    private def copy(
      sg: SocketGroup[F] = self.sg,
      host: Host = self.host,
      port: Port = self.port,
      tlsContext: Option[TLSContext[F]] = self.tlsContext,
      tlsParameters: TLSParameters = self.tlsParameters
    ): PooledConnectionBuilder[F] = new PooledConnectionBuilder(
      sg,
      host,
      port,
      tlsContext,
      tlsParameters
    )

    def withHost(host: Host) = copy(host = host)
    def withPort(port: Port) = copy(port = port)
    def withTLSContext(tlsContext: TLSContext[F]) = copy(tlsContext = tlsContext.some)
    def withoutTLSContext = copy(tlsContext = None)
    def withTLSParameters(tlsParameters: TLSParameters) = copy(tlsParameters = tlsParameters)
    def withSocketGroup(sg: SocketGroup[F]) = copy(sg = sg)

    def build: Resource[F,RedisConnection[F]] = 
      KeyPoolBuilder[F, Unit, (BufferedSocket[F], F[Unit])](
        {_ => sg.client(SocketAddress(host,port), Nil)
          .flatMap(elevateSocket(_, tlsContext, tlsParameters))
          .evalMap(s => BufferedSocket.fromSocket(s)).allocated
        },
        { case (_, shutdown) => shutdown}
      ).build.map(PooledConnection[F](_))
  }

  def queued[F[_]: Async]: QueuedConnectionBuilder[F] = 
    new QueuedConnectionBuilder(
      Network[F],
      Defaults.host,
      Defaults.port,
      None,
      TLSParameters.Default,
      Defaults.maxQueued,
      Defaults.workers,
      Defaults.chunkSizeLimit
    )

  class QueuedConnectionBuilder[F[_]: Async] private[RedisConnection](
    private val sg: SocketGroup[F],
    private val host: Host,
    private val port: Port,
    private val tlsContext: Option[TLSContext[F]],
    private val tlsParameters: TLSParameters,
    private val maxQueued: Int,
    private val workers: Int,
    private val chunkSizeLimit: Int,
  ) { self => 

    private def copy(
      sg: SocketGroup[F] = self.sg,
      host: Host = self.host,
      port: Port = self.port,
      tlsContext: Option[TLSContext[F]] = self.tlsContext,
      tlsParameters: TLSParameters = self.tlsParameters,
      maxQueued: Int = self.maxQueued,
      workers: Int = self.workers,
      chunkSizeLimit: Int = self.chunkSizeLimit,
    ): QueuedConnectionBuilder[F] = new QueuedConnectionBuilder(
      sg,
      host,
      port,
      tlsContext,
      tlsParameters,
      maxQueued,
      workers,
      chunkSizeLimit
    )

    def withHost(host: Host) = copy(host = host)
    def withPort(port: Port) = copy(port = port)
    def withTLSContext(tlsContext: TLSContext[F]) = copy(tlsContext = tlsContext.some)
    def withoutTLSContext = copy(tlsContext = None)
    def withTLSParameters(tlsParameters: TLSParameters) = copy(tlsParameters = tlsParameters)
    def withSocketGroup(sg: SocketGroup[F]) = copy(sg = sg)

    def withMaxQueued(maxQueued: Int) = copy(maxQueued = maxQueued)
    def withWorkers(workers: Int) = copy(workers = workers)
    def withChunkSizeLimit(chunkSizeLimit: Int) = copy(chunkSizeLimit = chunkSizeLimit)

    def build: Resource[F,RedisConnection[F]] = {
      for {
        queue <- Resource.eval(Queue.bounded[F, Chunk[(Either[Throwable,Resp] => F[Unit], Resp)]](maxQueued))
        keypool <- KeyPoolBuilder[F, Unit, (BufferedSocket[F], F[Unit])](
          {_ => sg.client(SocketAddress(host,port), Nil)
            .flatMap(elevateSocket(_, tlsContext, tlsParameters))
            .flatMap(s => Resource.eval(BufferedSocket.fromSocket(s)))
            .allocated
          },
          { case (_, shutdown) => shutdown}
        ).build
        _ <- 
            Stream.fromQueueUnterminatedChunk(queue, chunkSizeLimit).chunks.map{chunk =>
              val s = if (chunk.nonEmpty) {
                  Stream.eval(
                    Functor[({type M[A] = KeyPool[F, Unit, A]})#M].map(keypool)(_._1).take(()).attempt.use{
                      case Right(m) =>
                        val out = chunk.map(_._2)
                        explicitPipelineRequest(m.value, out).attempt.flatTap{// Currently Guarantee Chunk.size === returnSize
                          case Left(_) => m.canBeReused.set(Reusable.DontReuse)
                          case _ => Applicative[F].unit
                        }
                      case l@Left(_) => l.rightCast[Chunk[Resp]].pure[F]
                  }.flatMap{
                    case Right(n) => 
                      n.zipWithIndex.traverse_{
                        case (ref, i) => 
                          val (toSet, _) = chunk(i)
                          toSet(Either.right(ref))
                      }
                    case e@Left(_) => 
                      chunk.traverse_{ case (deff, _) => deff(e.asInstanceOf[Either[Throwable, Resp]])}
                  }) 
              } else {
                Stream.empty
              }
              s ++ Stream.exec(Concurrent[F].cede)
            }.parJoin(workers) // Worker Threads
            .compile
            .drain
            .background
      } yield Queued(queue, keypool.take(()).map(Functor[({type M[A] = Managed[F, A]})#M].map(_)(_._1)))
    }
  }

  def cluster[F[_]: Async]: ClusterConnectionBuilder[F] = 
    new ClusterConnectionBuilder(
      Network[F],
      Defaults.host,
      Defaults.port,
      None,
      TLSParameters.Default,
      Defaults.maxQueued,
      Defaults.workers,
      Defaults.chunkSizeLimit,
      Defaults.clusterParallelServerCalls,
      Defaults.clusterUseDynamicRefreshSource,
      Defaults.clusterCacheTopologySeconds
    )

  class ClusterConnectionBuilder[F[_]: Async] private[RedisConnection] (
    private val sg: SocketGroup[F],
    private val host: Host,
    private val port: Port,
    private val tlsContext: Option[TLSContext[F]],
    private val tlsParameters: TLSParameters,
    private val maxQueued: Int,
    private val workers: Int,
    private val chunkSizeLimit: Int,
    private val parallelServerCalls: Int = Int.MaxValue,
    private val useDynamicRefreshSource: Boolean = true, // Set to false to only use initially provided host for topology refresh
    private val cacheTopologySeconds: FiniteDuration = 1.second, // How long topology will not be rechecked for after a succesful refresh
  ) { self => 

    private def copy(
      sg: SocketGroup[F] = self.sg,
      host: Host = self.host,
      port: Port = self.port,
      tlsContext: Option[TLSContext[F]] = self.tlsContext,
      tlsParameters: TLSParameters = self.tlsParameters,
      maxQueued: Int = self.maxQueued,
      workers: Int = self.workers,
      chunkSizeLimit: Int = self.chunkSizeLimit,
      parallelServerCalls: Int = self.parallelServerCalls,
      useDynamicRefreshSource: Boolean = self.useDynamicRefreshSource,
      cacheTopologySeconds: FiniteDuration = self.cacheTopologySeconds
    ): ClusterConnectionBuilder[F] = new ClusterConnectionBuilder(
      sg,
      host,
      port,
      tlsContext,
      tlsParameters,
      maxQueued,
      workers,
      chunkSizeLimit,
      parallelServerCalls,
      useDynamicRefreshSource,
      cacheTopologySeconds
    )

    def withHost(host: Host) = copy(host = host)
    def withPort(port: Port) = copy(port = port)
    def withTLSContext(tlsContext: TLSContext[F]) = copy(tlsContext = tlsContext.some)
    def withoutTLSContext = copy(tlsContext = None)
    def withTLSParameters(tlsParameters: TLSParameters) = copy(tlsParameters = tlsParameters)
    def withSocketGroup(sg: SocketGroup[F]) = copy(sg = sg)

    def withMaxQueued(maxQueued: Int) = copy(maxQueued = maxQueued)
    def withWorkers(workers: Int) = copy(workers = workers)
    def withChunkSizeLimit(chunkSizeLimit: Int) = copy(chunkSizeLimit = chunkSizeLimit)

    def withParallelServerCalls(parallelServerCalls: Int) = copy(parallelServerCalls = parallelServerCalls)
    def withUseDynamicRefreshSource(useDynamicRefreshSource: Boolean) = copy(useDynamicRefreshSource = useDynamicRefreshSource)
    def withCacheTopologySeconds(cacheTopologySeconds: FiniteDuration) = copy(cacheTopologySeconds = cacheTopologySeconds)

    def build: Resource[F,RedisConnection[F]] = {
      for {
        keypool <- KeyPoolBuilder[F, (Host, Port), (BufferedSocket[F], F[Unit])](
          {(t: (Host, Port)) => sg.client(SocketAddress(host,port), Nil)
              .flatMap(elevateSocket(_, tlsContext, tlsParameters))
              .flatMap(s => Resource.eval(BufferedSocket.fromSocket(s)))
              .allocated
          },
          { case (_, shutdown) => shutdown}
        ).build

        // Cluster Topology Acquisition and Management
        sockets <- Resource.eval(keypool.take((host, port)).map(_.value._1).map(DirectConnection(_)).use(ClusterCommands.clusterslots[({ type M[A] = Redis[F, A] })#M].run(_)))
        now <- Resource.eval(Temporal[F].realTime.map(_.toMillis))
        refreshLock <- Resource.eval(Semaphore[F](1L))
        refTopology <- Resource.eval(Ref[F].of((sockets, now)))
        refreshTopology = refreshLock.permit.use(_ =>
          (
            refTopology.get
              .flatMap{ case (topo, setAt) => 
                if (useDynamicRefreshSource) 
                  Applicative[F].pure((NonEmptyList((host, port), topo.l.flatMap(c => c.replicas).map(r => (r.host, r.port))), setAt))
                else 
                  Applicative[F].pure((NonEmptyList.of((host, port)), setAt))
            },
            Temporal[F].realTime.map(_.toMillis)
          ).tupled
          .flatMap{
            case ((_, setAt), now) if setAt >= (now - cacheTopologySeconds.toMillis) => Applicative[F].unit
            case ((l, _), _) => 
              val nelActions: NonEmptyList[F[ClusterSlots]] = l.map{ case (host, port) => 
                keypool.take((host, port)).map(_.value._1).map(DirectConnection(_)).use(ClusterCommands.clusterslots[({ type M[A] = Redis[F, A] })#M].run(_))
              }
              raceNThrowFirst(nelActions)
                .flatMap(s => Clock[F].realTime.map(_.toMillis).flatMap(now => refTopology.set((s,now))))
          }
        )
        queue <- Resource.eval(Queue.bounded[F, Chunk[(Either[Throwable,Resp] => F[Unit], Option[String], Option[(Host, Port)], Int, Resp)]](maxQueued))
        cluster = Cluster(queue, refTopology.get.map(_._1), {case(host, port) => keypool.take((host, port)).map(_.map(_._1))})
        _ <- 
            Stream.fromQueueUnterminatedChunk(queue, chunkSizeLimit).chunks.map{chunk =>
              val s = if (chunk.nonEmpty) {
                Stream.eval(refTopology.get).map{ case (topo,_) => 
                  Stream.eval(topo.random[F]).flatMap{ default => 
                  Stream.emits(
                      chunk.toList.groupBy{ case (_, s, server,_,_) => // TODO Investigate Efficient Group By
                      server.orElse(s.flatMap(key => topo.served(HashSlot.find(key)))).getOrElse(default) // Explicitly Set Server, Key Hashslot Server, or a default server if none selected.
                    }.toSeq
                  ).evalMap{
                    case (server, rest) => 
                      Functor[({type M[A] = KeyPool[F, (Host, Port), A]})#M].map(keypool)(_._1).take(server).attempt.use{
                        case Right(m) =>
                          val out = Chunk.seq(rest.map(_._5))
                          explicitPipelineRequest(m.value, out).attempt.flatTap{// Currently Guarantee Chunk.size === returnSize
                            case Left(_) => m.canBeReused.set(Reusable.DontReuse)
                            case _ => Applicative[F].unit
                          }
                        case l@Left(_) => l.rightCast[Chunk[Resp]].pure[F]
                      }.flatMap{
                      case Right(n) => 
                        n.zipWithIndex.traverse_{
                          case (ref, i) => 
                            val (toSet, key, _, retries, initialCommand) = rest(i)
                            ref match {
                              case e@Resp.Error(s) if (s.startsWith("MOVED") && retries <= 5)  => // MOVED 1234-2020 127.0.0.1:6381
                                refreshTopology.attempt.void >>
                                // Offer To Have it reprocessed. 
                                // If the queue is full return the error to the user
                                cluster.queue.tryOffer(Chunk.singleton((toSet, key, extractServer(s), retries + 1, initialCommand)))
                                  .ifM( 
                                    Applicative[F].unit,
                                    toSet(Either.right(e)).void
                                  )
                              case e@Resp.Error(s) if (s.startsWith("ASK") && retries <= 5) => // ASK 1234-2020 127.0.0.1:6381
                                val serverRedirect = extractServer(s)
                                serverRedirect match {
                                  case s@Some(_) => // This is a Special One Off, Requires a Redirect
                                    // Deferred[F, Either[Throwable, Resp]].flatMap{d => // No One Cares About this Callback
                                      val asking = ({(_: Either[Throwable, Resp]) => Applicative[F].unit}, key, s, 6, Resp.renderRequest(NonEmptyList.of("ASKING"))) // Never Repeat Asking
                                      val repeat = (toSet, key, s, retries + 1, initialCommand)
                                      val chunk = Chunk(asking, repeat)
                                      cluster.queue.tryOffer(chunk) // Offer To Have it reprocessed. 
                                        //If the queue is full return the error to the user
                                        .ifM(
                                          Applicative[F].unit,
                                          toSet(Either.right(e))
                                        )
                                    // }
                                  case None => 
                                    toSet(Either.right(e))
                                }
                              case otherwise => 
                                toSet(Either.right(otherwise))
                            }
                        }
                      case e@Left(_) =>
                        refreshTopology.attempt.void >>
                        rest.traverse_{ case (deff, _, _, _, _) => deff(e.asInstanceOf[Either[Throwable, Resp]])}
                    }

                  }
                }}.parJoin(parallelServerCalls) // Send All Acquired values simultaneously. Should be mostly IO awaiting callback
              } else Stream.empty
              s ++ Stream.exec(Async[F].cede)
            }.parJoin(workers)
              .compile
              .drain
              .background
      } yield cluster
    }
  }

  private def elevateSocket[F[_]](socket: Socket[F], tlsContext: Option[TLSContext[F]], tlsParameters: TLSParameters): Resource[F, Socket[F]] = 
    tlsContext.fold(Resource.pure[F, Socket[F]](socket))(c => c.clientBuilder(socket).withParameters(tlsParameters).build)

  // ASK 1234-2020 127.0.0.1:6381
  // MOVED 1234-2020 127.0.0.1:6381
  private def extractServer(s: String): Option[(Host, Port)] = {
    val end = s.lastIndexOf(' ')
    val portSplit = s.lastIndexOf(':')
    if (end > 0 &&  portSplit >= end + 1){
      val host = s.substring(end + 1, portSplit)
      val port = s.substring(portSplit +1, s.length())
      for {
        h <- Host.fromString(host)
        pI <- Either.catchNonFatal(port.toInt).toOption
        port <- Port.fromInt(pI)
      } yield (h, port)
    } else None
  }

  private def raceNThrowFirst[F[_]: Concurrent, A](nel: NonEmptyList[F[A]]): F[A] = 
    Stream(Stream.emits(nel.toList).evalMap(identity)).covary[F].parJoinUnbounded.take(1).compile.lastOrError
}