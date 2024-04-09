package io.chrisdavenport.rediculous

import scala.concurrent.duration.Duration
import java.util.concurrent.TimeoutException

/** Indicates a Error while processing for Rediculous */
trait RedisError extends RuntimeException {

  /** Provides a message appropriate for logging. */
  def message: String

  /* Overridden for sensible logging of the failure */
  final override def getMessage: String = message

  def cause: Option[Throwable]

  final override def getCause: Throwable = cause.orNull
}

object RedisError {
  final case class Generic(message: String) extends RedisError{
    val cause: Option[Throwable] = None
  }

  final case class QueuedExceptionError(baseCase: Throwable) extends RedisError {
    override val message: String = s"Error encountered in queue: ${baseCase.getMessage()}"
    override val cause: Option[Throwable] = Some(baseCase)
  }

  // TODO
  trait RedisTimeoutException

  final case class CommandTimeoutException(timeout: Duration) extends TimeoutException(s"Redis Command Timed Out: $timeout") with RedisTimeoutException
  final case class RedisRequestTimeoutException(timeout: Duration) extends TimeoutException(s"Redis Request Timed Out: $timeout") with RedisTimeoutException

}