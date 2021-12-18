package io.chrisdavenport.rediculous

import cats.implicits._
import fs2.{Stream, Pipe}
import scala.concurrent.duration.Duration
import RedisCommands.{XAddOpts, XReadOpts, StreamOffset, Trimming, xadd, xread}
import cats.effect._

final case class XAddMessage(key: String, body: Map[String, String], approxMaxlen: Option[Long] = None)
final case class XReadMessage(id: MessageId, key: String, body: Map[String, String])
final case class MessageId(value: String) extends AnyVal

trait RedisStream[F[_]] {
  def append(messages: List[XAddMessage]): F[List[MessageId]]

  def read(
    keys: Set[String],
    chunkSize: Int,
    initialOffset: String => StreamOffset = StreamOffset.All,
    block: Duration = Duration.Zero,
    count: Option[Long] = None
  ): Stream[F, XReadMessage]
}

object RedisStream {
  /**
   * Create a RedisStream from a connection.
   * 
   **/
  def fromConnection[F[_]: Async](connection: RedisConnection[F]): RedisStream[F] = new RedisStream[F] {
    def append(messages: List[XAddMessage]): F[List[MessageId]] = {
      messages
        .traverse{ msg => 
          val opts = msg.approxMaxlen.map(l => XAddOpts.default.copy(maxLength = l.some, trimming = Trimming.Approximate.some))
          xadd[RedisPipeline](msg.key, msg.body, opts getOrElse XAddOpts.default).map(MessageId.apply)
        }
        .pipeline[F]
        .run(connection)
    }

    private val nextOffset: String => XReadMessage => StreamOffset = 
      key => msg => StreamOffset.From(key, msg.id.value)

    private val offsetsByKey: List[XReadMessage] => Map[String, Option[StreamOffset]] =
      list => list.groupBy(_.key).map { case (k, values) => k -> values.lastOption.map(nextOffset(k)) }

    def read(keys: Set[String], chunkSize: Int, initialOffset: String => StreamOffset, block: Duration, count: Option[Long]): Stream[F, XReadMessage] = {
      val initial = keys.map(k => k -> initialOffset(k)).toMap
      val opts = XReadOpts.default.copy(blockMillisecond = block.toMillis.some, count = count)
      Stream.eval(Ref.of[F, Map[String, StreamOffset]](initial)).flatMap { ref =>
        (for {
          offsets <- Stream.eval(ref.get)
          list <- Stream.eval(xread(offsets.values.toSet, opts).run(connection)).flattenOption
          messages = list.flatten.map { case (k, l) => l.flatten.map{case (id, values) => XReadMessage(MessageId(id), k, values.toMap)}}.flatten
          newOffsets = offsetsByKey(messages).collect { case (key, Some(value)) => key -> value }.toList
          _ <- Stream.eval(newOffsets.map { case (k, v) => ref.update(_.updated(k, v)) }.sequence)
          result <- Stream.fromIterator(messages.iterator, chunkSize)
        } yield result).repeat
      }
    }
  }
}