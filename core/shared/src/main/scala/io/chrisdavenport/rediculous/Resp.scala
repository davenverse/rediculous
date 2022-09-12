package io.chrisdavenport.rediculous

import scala.collection.mutable
import cats.data.NonEmptyList
import cats.implicits._
import scala.util.control.NonFatal
import java.nio.charset.StandardCharsets
import java.nio.charset.Charset
import scodec.bits.ByteVector

import scodec.Codec
import scodec.bits.{BitVector, ByteVector}
import scodec.codecs._
import scodec.Attempt.Failure
import scodec.Attempt.Successful
import scodec.Attempt
import scodec.Err
import fs2.Chunk

sealed trait Resp extends Product with Serializable

object Resp {

  // First Byte is +
  // +foo/r/n
  case class SimpleString(value: String) extends Resp

  // First Byte is -
  case class Error(value: String) extends RedisError with Resp{
    def message: String = s"Error($value)"
    val cause: Option[Throwable] = None
  }

  // First Byte is :5412481/r/n
  case class Integer(long: Long) extends Resp

  // First Byte is $
  // $3/r/n/foo/r/n
  case class BulkString(value: Option[ByteVector]) extends Resp

  // First Byte is *
  case class Array(a: Option[List[Resp]]) extends Resp

  def renderRequest(nel: NonEmptyList[ByteVector]): Resp = {
    Resp.Array(Some(
      nel.toList.map(renderArg)
    ))
  }

  def renderArg(arg: ByteVector): Resp = {
    Resp.BulkString(Some(arg))
  }

  def toStringProtocol(resp: Resp)(implicit C: Charset = StandardCharsets.UTF_8) = {
    CodecUtils.codec.encode(resp).toEither
      .leftMap(err => new Throwable(s"Failed Encoding $err"))
      .flatMap(bits => bits.bytes.decodeString)
      .fold(throw _, identity(_))
  }

  def toStringRedisCLI(resp: Resp, depth: Int = 0): String = resp match {
    case BulkString(Some(value)) => s""""$value""""
    case BulkString(None) => "(empty bulk string)"
    case SimpleString(value) => s""""$value""""
    case Integer(long) => s"(integer) $long"
    case Error(value) => s"(error) $value"
    case Array(None) => "(empty array)"
    case Array(Some(a)) => 
      a.zipWithIndex.map{ case (a, i) => (a, i + 1)}
        .map{ case (resp, i) => 
          val whitespace = if (i > 1) List.fill(depth * 3)(" ").mkString  else ""
          whitespace ++ s"$i) ${toStringRedisCLI(resp, depth + 1)}"
        }.mkString("\n")
  }

  object CodecUtils {
    private val asciiInt: Codec[scala.Int] = ascii.xmap(_.toInt, _.toString())
    private val asciiLong: Codec[scala.Long] = ascii.xmap(_.toLong, _.toString())
    private val crlf = BitVector('\r', '\n')
    private val delimInt: Codec[scala.Int] = crlfTerm(asciiInt).withContext("delimInt")
    private val delimLong: Codec[Long] = crlfTerm(asciiLong)

    lazy val codec: Codec[Resp] =
      discriminated[Resp].by(byte)
        .typecase('+', crlfTerm(utf8).as[SimpleString].withContext("SimpleString"))
        .typecase('-', crlfTerm(utf8).as[Error].withContext("Error"))
        .typecase(':', delimLong.as[Integer].withContext("Integer"))
        .typecase('$', bulk0)
        .typecase('*', array0)

    private val constEmpty = ByteVector('1', '\r', '\n')
    private lazy val bulk0: Codec[BulkString] =
      discriminated[BulkString].by(recover(constant('-'))) // -1\r\n
        .caseP(true){ case BulkString(None) => ()}{bv => BulkString(None)}(constant('1', '\r', '\n').withContext("BulkString None"))
        .caseP(false){ case BulkString(Some(s)) => s}{ case bv => BulkString(Some(bv))}((variableSizeBytes(delimInt, bytes) <~ constant(crlf)).withContext("BulkString Some"))

    // lazy val array: Codec[Array] = constant('*') ~> array0

    private lazy val array0: Codec[Array] =
      discriminated[Array].by(recover(constant('-')))
        .caseP(true){ case Array(None) => constEmpty}(_ => Array(None))(bytes.withContext("Array Nil"))
        .caseP(false){ case Array(Some(s)) => s}{ case l => Array(Some(l))}(listOfN(delimInt, lazily(codec)).withContext("Array Some"))

    // CRLF are much harder to see visually
    private def flatEncode(s: String): String = s.replace("\r", "\\r").replace("\n", "\\n")

    private def crlfTerm[A](inner: Codec[A]): Codec[A] =
      Codec(
        inner.encode(_).map(_ ++ crlf),
        { bits =>
          val bytes = bits.bytes

          var i = 0L
          var done = false
          while (i < bytes.size - 1 && !done) {
            if (bytes(i) == '\r' && bytes(i + 1) == '\n')
              done = true
            else
              i += 1
          }
          if (done) {
            val (front, back) = bytes.splitAt(i)
            val decoded = inner.decode(front.bits)
            // println(s"bits = $bits;\n bits.decodeAscii = ${bits.decodeAscii.map(flatEncode)};\n front = ${front.decodeAscii.map(flatEncode)};\n i = $i;\n decoded=$decoded")
            decoded.map(_.copy(remainder = back.drop(2).bits))
          } else Attempt.Failure(Err.insufficientBits(-1, bytes.length)) // Not a great method, but we don't have enough for the crlf and this keeps us looking
        }
      )
  }
}