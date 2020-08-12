package io.chrisdavenport.rediculous

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
}