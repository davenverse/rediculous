package io.chrisdavenport.rediculous

import cats.implicits._
import fs2.{Stream, Pipe, Chunk}
import scala.concurrent.duration.Duration
import RedisCommands.{XAddOpts, XReadOpts, StreamOffset, Trimming, xadd, xread}
import cats.effect._


trait RedisStream[F[_]] {
  
  def append(messages: Chunk[RedisStream.XAddMessage]): F[Chunk[Resp]]

  def read(
    streams: Set[String],
    initialOffset: String => StreamOffset = {(s: String) => StreamOffset.All(s)},
    block: Duration = Duration.Zero,
    count: Option[Long] = Some(512)
  ): Stream[F, RedisCommands.XReadResponse]
}

object RedisStream {

  final case class XAddMessage(stream: String, body: List[(String, String)], approxMaxlen: Option[Long] = None)

  /**
   * Create a RedisStream from a connection.
   * 
   **/
  def fromConnection[F[_]: Concurrent](connection: RedisConnection[F]): RedisStream[F] = new RedisStream[F] {
    def append(messages: Chunk[XAddMessage]): F[Chunk[Resp]] = {
      messages
        .traverse{ case msg => 
          val opts = msg.approxMaxlen.map(l => XAddOpts.default.copy(maxLength = l.some, trimming = Trimming.Approximate.some))
          xadd[RespRaw.RawPipeline](msg.stream, msg.body, opts getOrElse XAddOpts.default)
        }
        .pipeline(connection)
    }

    private val nextOffset: String => RedisCommands.StreamsRecord => StreamOffset = 
      key => msg => StreamOffset.From(key, msg.recordId)

    private val offsetsByKey: List[RedisCommands.XReadResponse] => Map[String, Option[StreamOffset]] =
      list => list.groupBy(_.stream).map { case (k, values) => k -> values.flatMap(_.records).lastOption.map(nextOffset(k)) }

    def read(keys: Set[String],  initialOffset: String => StreamOffset, block: Duration, count: Option[Long]): Stream[F, RedisCommands.XReadResponse] = {
      val initial = keys.map(k => k -> initialOffset(k)).toMap
      val opts = XReadOpts.default.copy(blockMillisecond = block.toMillis.some, count = count)
      Stream.eval(Ref.of[F, Map[String, StreamOffset]](initial)).flatMap { ref =>
        (for {
          offsets <- Stream.eval(ref.get)
          list <- Stream.eval(xread(offsets.values.toSet, opts).run(connection)).flattenOption
          newOffsets = offsetsByKey(list).collect { case (key, Some(value)) => key -> value }.toList
          _ <- Stream.eval(newOffsets.map { case (k, v) => ref.update(_.updated(k, v)) }.sequence)
          result <- Stream.emits(list)
        } yield result).repeat
      }
    }
  }
} 