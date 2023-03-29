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
import fs2.io.net.Socket
import fs2.io.net.tls.TLSContext
import fs2.io.net.tls.TLSParameters
import java.time.Instant
import _root_.io.chrisdavenport.rediculous.cluster.ClusterCommands.ClusterSlots
import fs2.io.net.SocketGroupCompanionPlatform
import scodec.bits.ByteVector
import fs2.io.net.Network

trait RedisConnection[F[_]]{
  def runRequest(
    inputs: Chunk[NonEmptyList[ByteVector]],
    key: Option[ByteVector]
  ): F[Chunk[Resp]]
}
object RedisConnection{
  
  private[rediculous] case class Queued[F[_]: Concurrent](queue: Queue[F, Chunk[(Either[Throwable, Resp] => F[Unit], Resp)]], usePool: Resource[F, Managed[F, Socket[F]]]) extends RedisConnection[F]{
    def runRequest(inputs: Chunk[NonEmptyList[ByteVector]], key: Option[ByteVector]): F[Chunk[Resp]] = {
      val chunk = Chunk.seq(inputs.toList.map(Resp.renderRequest))
      chunk.traverse(resp => Deferred[F, Either[Throwable, Resp]].map(d => (d, ({(e: Either[Throwable, Resp]) => d.complete(e).void}, resp)))).flatMap{ c => 
        queue.offer(c.map(_._2)) >> {
          val x: F[Chunk[Either[Throwable, Resp]]] = c.traverse{ case (d, _) => d.get }
          val y: F[Chunk[Resp]] = x.flatMap(_.sequence.liftTo[F].adaptError{case e => RedisError.QueuedExceptionError(e)})
          y
        }
      }
    } 
  }
  private[rediculous] case class PooledConnection[F[_]: Concurrent](
    pool: KeyPool[F, Unit, (Socket[F], F[Unit])]
  ) extends RedisConnection[F]{
    def runRequest(inputs: Chunk[NonEmptyList[ByteVector]], key: Option[ByteVector]): F[Chunk[Resp]] = {
      val chunk = Chunk.seq(inputs.toList.map(Resp.renderRequest))
      def withSocket(socket: Socket[F]): F[Chunk[Resp]] = explicitPipelineRequest[F](socket, chunk)
      Functor[KeyPool[F, Unit, *]].map(pool)(_._1).take(()).use{
        m => withSocket(m.value).attempt.flatTap{
          case Left(_) => m.canBeReused.set(Reusable.DontReuse)
          case _ => Applicative[F].unit
        }
      }.rethrow
    }
  }

  private[rediculous] case class DirectConnection[F[_]: Concurrent](socket: Socket[F]) extends RedisConnection[F]{
    def runRequest(inputs: Chunk[NonEmptyList[ByteVector]], key: Option[ByteVector]): F[Chunk[Resp]] = {
      val chunk = Chunk.seq(inputs.toList.map(Resp.renderRequest))
      def withSocket(socket: Socket[F]): F[Chunk[Resp]] = explicitPipelineRequest[F](socket, chunk)
      withSocket(socket)
    }
  }

  private[rediculous] case class Cluster[F[_]: Concurrent](queue: Queue[F, Chunk[(Either[Throwable, Resp] => F[Unit], Option[ByteVector], Option[(Host, Port)], Int, Resp)]], slots: F[ClusterSlots], usePool: (Host, Port) => Resource[F, Managed[F, Socket[F]]]) extends RedisConnection[F]{
    def runRequest(inputs: Chunk[NonEmptyList[ByteVector]], key: Option[ByteVector]): F[Chunk[Resp]] = {
      val chunk = Chunk.seq(inputs.toList.map(Resp.renderRequest))
      chunk.traverse(resp => Deferred[F, Either[Throwable, Resp]].map(d => (d, ({(e: Either[Throwable, Resp]) => d.complete(e).void}, key, None, 0, resp)))).flatMap{ c => 
        queue.offer(c.map(_._2)) >> {
          c.traverse(_._1.get).flatMap(_.sequence.liftTo[F].adaptError{case e => RedisError.QueuedExceptionError(e)})
        }
      }   
    }
  }

  // Guarantees With Socket That Each Call Receives a Response
  // Chunk must be non-empty but to do so incurs a penalty
  private[rediculous] def explicitPipelineRequest[F[_]: Concurrent](socket: Socket[F], calls: Chunk[Resp], maxBytes: Int = 16 * 1024 * 1024): F[Chunk[Resp]] = {
    val out = calls.flatMap(resp =>
      Resp.CodecUtils.codec.encode(resp).toEither.traverse(bits => Chunk.byteVector(bits.bytes))
    ).sequence.leftMap(err => new Throwable(s"Failed To Encode Response $err")).liftTo[F]
    out.flatMap(socket.write) >> 
    Stream.eval(socket.read(maxBytes)).repeat.unNoneTerminate.unchunks.through(fs2.interop.scodec.StreamDecoder.many(Resp.CodecUtils.codec).toPipeByte)
      .take(calls.size)
      .compile
      .to(Chunk)
  }

  def runRequestInternal[F[_]: Concurrent](connection: RedisConnection[F])(
    inputs: Chunk[NonEmptyList[ByteVector]],
    key: Option[ByteVector]
  ): F[Chunk[Resp]] = connection.runRequest(inputs, key)

  def toNel[F[_]: ApplicativeThrow](chunk: Chunk[Resp]): F[NonEmptyList[Resp]] = 
    chunk.toNel.liftTo[F](RedisError.Generic("Rediculous: Impossible Return List was Empty but we guarantee output matches input"))

  def head[F[_]: ApplicativeThrow](chunk: Chunk[Resp]): F[Resp] = 
    chunk.head.liftTo[F](RedisError.Generic("Rediculous: Impossible Return List was Empty but we guarantee output matches input"))

  // Can Be used to implement any low level protocols.
  def runRequest[F[_]: Concurrent, A: RedisResult](connection: RedisConnection[F])(input: NonEmptyList[ByteVector], key: Option[ByteVector]): F[Either[Resp, A]] = 
    runRequestInternal(connection)(Chunk.singleton(input), key).flatMap(head[F]).map(resp => RedisResult[A].decode(resp))

  def runRequestTotal[F[_]: Concurrent, A: RedisResult](input: NonEmptyList[ByteVector], key: Option[ByteVector]): Redis[F, A] = Redis(Kleisli{(connection: RedisConnection[F]) => 
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
    val useTLS: Boolean = false
  }

  def direct[F[_]: Temporal: Network]: DirectConnectionBuilder[F] =
    new DirectConnectionBuilder(
      Network[F],
      Defaults.host,
      Defaults.port,
      None,
      TLSParameters.Default,
      None,
      Defaults.useTLS
    )

  @deprecated("Use overload that takes a Network", "0.4.1")
  def direct[F[_]](F: Async[F]): DirectConnectionBuilder[F] =
    direct(F, Network.forAsync(F))

  class DirectConnectionBuilder[F[_]: Temporal: Network] private[RedisConnection](
    private val sg: SocketGroup[F],
    val host: Host,
    val port: Port,
    private val tlsContext: Option[TLSContext[F]],
    private val tlsParameters: TLSParameters,
    private val auth: Option[(Option[String], String)],
    private val useTLS: Boolean,
  ) { self => 

    private def copy(
      sg: SocketGroup[F] = self.sg,
      host: Host = self.host,
      port: Port = self.port,
      tlsContext: Option[TLSContext[F]] = self.tlsContext,
      tlsParameters: TLSParameters = self.tlsParameters,
      auth: Option[(Option[String], String)] = self.auth,
      useTLS: Boolean = self.useTLS
    ): DirectConnectionBuilder[F] = new DirectConnectionBuilder(
      sg,
      host,
      port,
      tlsContext,
      tlsParameters,
      auth,
      useTLS
    )

    def withHost(host: Host) = copy(host = host)
    def withPort(port: Port) = copy(port = port)
    def withTLSContext(tlsContext: TLSContext[F]) = copy(tlsContext = tlsContext.some)
    def withoutTLSContext = copy(tlsContext = None)
    def withTLSParameters(tlsParameters: TLSParameters) = copy(tlsParameters = tlsParameters)
    def withSocketGroup(sg: SocketGroup[F]) = copy(sg = sg)
    def withAuth(username: Option[String], password: String) = copy(auth = Some((username, password)))
    def withoutAuth = copy(auth = None)
    def withTLS = copy(useTLS = true)
    def withoutTLS = copy(useTLS = false)

    def build: Resource[F,RedisConnection[F]] = 
      for {
        socket <- sg.client(SocketAddress(host,port), Nil)
        tlsContextOptWithDefault <-
          tlsContext
            .fold(Network[F].tlsContext.systemResource.attempt.map(_.toOption))(
              _.some.pure[Resource[F, *]]
            )
        out <- elevateSocket(socket, tlsContextOptWithDefault, tlsParameters, useTLS)
        _ <- Resource.eval(auth match {
          case None => ().pure[F]
          case Some((Some(username), password)) =>
            RedisCommands.auth[Redis[F, *]](username, password).run(DirectConnection(out)).void
          case Some((None, password)) =>
            RedisCommands.auth[Redis[F, *]](password).run(DirectConnection(out)).void
        })
      } yield RedisConnection.DirectConnection(out)
  }

  def pool[F[_]: Temporal: Network]: PooledConnectionBuilder[F] =
    new PooledConnectionBuilder(
      Network[F],
      Defaults.host,
      Defaults.port,
      None,
      TLSParameters.Default,
      None,
      Defaults.useTLS,
    )

  @deprecated("Use overload that takes a Network", "0.4.1")
  def pool[F[_]](F: Async[F]): PooledConnectionBuilder[F] =
    pool(F, Network.forAsync(F))

  class PooledConnectionBuilder[F[_]: Temporal: Network] private[RedisConnection] (
    private val sg: SocketGroup[F],
    val host: Host,
    val port: Port,
    private val tlsContext: Option[TLSContext[F]],
    private val tlsParameters: TLSParameters,
    private val auth: Option[(Option[String], String)],
    private val useTLS: Boolean,
  ) { self =>

    private def copy(
      sg: SocketGroup[F] = self.sg,
      host: Host = self.host,
      port: Port = self.port,
      tlsContext: Option[TLSContext[F]] = self.tlsContext,
      tlsParameters: TLSParameters = self.tlsParameters,
      auth: Option[(Option[String], String)] = self.auth,
      useTLS: Boolean = self.useTLS,
    ): PooledConnectionBuilder[F] = new PooledConnectionBuilder(
      sg,
      host,
      port,
      tlsContext,
      tlsParameters,
      auth,
      useTLS
    )

    def withHost(host: Host) = copy(host = host)
    def withPort(port: Port) = copy(port = port)
    def withTLSContext(tlsContext: TLSContext[F]) = copy(tlsContext = tlsContext.some)
    def withoutTLSContext = copy(tlsContext = None)
    def withTLSParameters(tlsParameters: TLSParameters) = copy(tlsParameters = tlsParameters)
    def withSocketGroup(sg: SocketGroup[F]) = copy(sg = sg)
    def withAuth(username: Option[String], password: String) = copy(auth = Some((username, password)))
    def withoutAuth = copy(auth = None)
    def withTLS = copy(useTLS = true)
    def withoutTLS = copy(useTLS = false)

    def build: Resource[F,RedisConnection[F]] = for {
      tlsContextOptWithDefault <-
        tlsContext
          .fold(Network[F].tlsContext.systemResource.attempt.map(_.toOption))(
            _.some.pure[Resource[F, *]]
          )
      kp <- KeyPoolBuilder[F, Unit, (Socket[F], F[Unit])](
        {_ => sg.client(SocketAddress(host,port), Nil)
          .flatMap(elevateSocket(_, tlsContextOptWithDefault, tlsParameters, useTLS))
          .evalTap(socket =>
            auth match {
              case None => ().pure[F]
              case Some((Some(username), password)) =>
                RedisCommands.auth[Redis[F, *]](username, password).run(DirectConnection(socket)).void
              case Some((None, password)) =>
                RedisCommands.auth[Redis[F, *]](password).run(DirectConnection(socket)).void
            }
          )
          .allocated
        },
        { case (_, shutdown) => shutdown}
      ).build
    } yield PooledConnection[F](kp)

  }

  def queued[F[_]: Temporal: Network]: QueuedConnectionBuilder[F] =
    new QueuedConnectionBuilder(
      Network[F],
      Defaults.host,
      Defaults.port,
      None,
      TLSParameters.Default,
      Defaults.maxQueued,
      Defaults.workers,
      Defaults.chunkSizeLimit,
      None,
      Defaults.useTLS,
    )

  @deprecated("Use overload that takes a Network", "0.4.1")
  def queued[F[_]](F: Async[F]): QueuedConnectionBuilder[F] =
    queued(F, Network.forAsync(F))

  class QueuedConnectionBuilder[F[_]: Temporal: Network] private[RedisConnection](
    private val sg: SocketGroup[F],
    val host: Host,
    val port: Port,
    private val tlsContext: Option[TLSContext[F]],
    private val tlsParameters: TLSParameters,
    private val maxQueued: Int,
    private val workers: Int,
    private val chunkSizeLimit: Int,
    private val auth: Option[(Option[String], String)],
    private val useTLS: Boolean,
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
      auth: Option[(Option[String], String)] = self.auth,
      useTLS: Boolean = self.useTLS,
    ): QueuedConnectionBuilder[F] = new QueuedConnectionBuilder(
      sg,
      host,
      port,
      tlsContext,
      tlsParameters,
      maxQueued,
      workers,
      chunkSizeLimit,
      auth,
      useTLS,
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
    def withAuth(username: Option[String], password: String) = copy(auth = Some((username, password)))
    def withoutAuth = copy(auth = None)

    def withTLS = copy(useTLS = true)
    def withoutTLS = copy(useTLS = false)

    def build: Resource[F,RedisConnection[F]] = {
      for {
        queue <- Resource.eval(Queue.bounded[F, Chunk[(Either[Throwable,Resp] => F[Unit], Resp)]](maxQueued))

        tlsContextOptWithDefault <-
          tlsContext
            .fold(Network[F].tlsContext.systemResource.attempt.map(_.toOption))(
              _.some.pure[Resource[F, *]]
            )
        keypool <- KeyPoolBuilder[F, Unit, (Socket[F], F[Unit])](
          {_ => sg.client(SocketAddress(host,port), Nil)
            .flatMap(elevateSocket(_, tlsContextOptWithDefault, tlsParameters, useTLS))
            .evalTap(socket =>
              auth match {
                case None => ().pure[F]
                case Some((Some(username), password)) =>
                  RedisCommands.auth[Redis[F, *]](username, password).run(DirectConnection(socket)).void
                case Some((None, password)) =>
                  RedisCommands.auth[Redis[F, *]](password).run(DirectConnection(socket)).void
              }
            )
            .allocated
          },
          { case (_, shutdown) => shutdown}
        ).build
        _ <- 
            Stream.fromQueueUnterminatedChunk(queue, chunkSizeLimit).chunks.map{chunk =>
              val s = if (chunk.nonEmpty) {
                  Stream.eval(
                    Functor[KeyPool[F, Unit, *]].map(keypool)(_._1).take(()).attempt.use{
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
      } yield Queued(queue, keypool.take(()).map(Functor[Managed[F, *]].map(_)(_._1)))
    }
  }

  def cluster[F[_]: Async: Network]: ClusterConnectionBuilder[F] =
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
      Defaults.clusterCacheTopologySeconds,
      None,
      Defaults.useTLS,
    )

  @deprecated("Use overload that takes a Network", "0.4.1")
  def cluster[F[_]](F: Async[F]): ClusterConnectionBuilder[F] =
    cluster(F, Network.forAsync(F))

  class ClusterConnectionBuilder[F[_]: Async: Network] private[RedisConnection] (
    private val sg: SocketGroup[F],
    val host: Host,
    val port: Port,
    private val tlsContext: Option[TLSContext[F]],
    private val tlsParameters: TLSParameters,
    private val maxQueued: Int,
    private val workers: Int,
    private val chunkSizeLimit: Int,
    private val parallelServerCalls: Int,
    private val useDynamicRefreshSource: Boolean, // Set to false to only use initially provided host for topology refresh
    private val cacheTopologySeconds: FiniteDuration, // How long topology will not be rechecked for after a succesful refresh
    private val auth: Option[(Option[String], String)],
    private val useTLS: Boolean,
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
      cacheTopologySeconds: FiniteDuration = self.cacheTopologySeconds,
      auth: Option[(Option[String], String)] = self.auth,
      useTLS: Boolean = self.useTLS,
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
      cacheTopologySeconds,
      auth,
      useTLS
    )

    def withHost(host: Host) = copy(host = host)
    def withPort(port: Port) = copy(port = port)
    def withTLSContext(tlsContext: TLSContext[F]) = copy(tlsContext = tlsContext.some)
    def withoutTLSContext = copy(tlsContext = None)
    def withTLSParameters(tlsParameters: TLSParameters) = copy(tlsParameters = tlsParameters)
    def withSocketGroup(sg: SocketGroup[F]) = copy(sg = sg)

    def withAuth(username: Option[String], password: String) = copy(auth = Some((username, password)))
    def withoutAuth = copy(auth = None)

    def withMaxQueued(maxQueued: Int) = copy(maxQueued = maxQueued)
    def withWorkers(workers: Int) = copy(workers = workers)
    def withChunkSizeLimit(chunkSizeLimit: Int) = copy(chunkSizeLimit = chunkSizeLimit)

    def withParallelServerCalls(parallelServerCalls: Int) = copy(parallelServerCalls = parallelServerCalls)
    def withUseDynamicRefreshSource(useDynamicRefreshSource: Boolean) = copy(useDynamicRefreshSource = useDynamicRefreshSource)
    def withCacheTopologySeconds(cacheTopologySeconds: FiniteDuration) = copy(cacheTopologySeconds = cacheTopologySeconds)

    def withTLS = copy(useTLS = true)
    def withoutTLS = copy(useTLS = false)

    def build: Resource[F,RedisConnection[F]] = {
      for {
        tlsContextOptWithDefault <-
          tlsContext
            .fold(Network[F].tlsContext.systemResource.attempt.map(_.toOption))(
              _.some.pure[Resource[F, *]]
            )
        keypool <- KeyPoolBuilder[F, (Host, Port), (Socket[F], F[Unit])](
          {(t: (Host, Port)) => sg.client(SocketAddress(host,port), Nil)
              .flatMap(elevateSocket(_, tlsContextOptWithDefault, tlsParameters, useTLS))
              .evalTap(socket =>
                auth match {
                  case None => ().pure[F]
                  case Some((Some(username), password)) =>
                    RedisCommands.auth[Redis[F, *]](username, password).run(DirectConnection(socket)).void
                  case Some((None, password)) =>
                    RedisCommands.auth[Redis[F, *]](password).run(DirectConnection(socket)).void
                }
              )
              .allocated
          },
          { case (_, shutdown) => shutdown}
        ).build

        // Cluster Topology Acquisition and Management
        sockets <- Resource.eval(keypool.take((host, port)).map(_.value._1).map(DirectConnection(_)).use(ClusterCommands.clusterslots[Redis[F, *]].run(_)))
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
                keypool.take((host, port)).map(_.value._1).map(DirectConnection(_)).use(ClusterCommands.clusterslots[Redis[F, *]].run(_))
              }
              raceNThrowFirst(nelActions)
                .flatMap(s => Clock[F].realTime.map(_.toMillis).flatMap(now => refTopology.set((s,now))))
          }
        )
        queue <- Resource.eval(Queue.bounded[F, Chunk[(Either[Throwable,Resp] => F[Unit], Option[ByteVector], Option[(Host, Port)], Int, Resp)]](maxQueued))
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
                      Functor[KeyPool[F, (Host, Port), *]].map(keypool)(_._1).take(server).attempt.use{
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
                                      val asking = ({(_: Either[Throwable, Resp]) => Applicative[F].unit}, key, s, 6, Resp.renderRequest(NonEmptyList.of(ByteVector.encodeAscii("ASKING").fold(throw _, identity(_))))) // Never Repeat Asking
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
              s ++ Stream.exec(Concurrent[F].cede)
            }.parJoin(workers)
              .compile
              .drain
              .background
      } yield cluster
    }
  }

  private def elevateSocket[F[_]](socket: Socket[F], tlsContext: Option[TLSContext[F]], tlsParameters: TLSParameters, useTLS: Boolean): Resource[F, Socket[F]] =
    tlsContext.fold(Resource.pure[F, Socket[F]](socket))(c => if (!useTLS) Resource.pure[F, Socket[F]](socket) else c.clientBuilder(socket).withParameters(tlsParameters).build)

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