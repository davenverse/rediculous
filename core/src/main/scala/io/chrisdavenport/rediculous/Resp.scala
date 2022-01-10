package io.chrisdavenport.rediculous

import scala.collection.mutable
import cats.data.NonEmptyList
import cats.implicits._
import scala.util.control.NonFatal
import java.nio.charset.StandardCharsets
import java.nio.charset.Charset
import scodec.bits.ByteVector

sealed trait Resp

object Resp {
  import scala.{Array => SArray}
  sealed trait RespParserResult[+A]{
    def extract: Option[A] = this match {
      case ParseComplete(out, _) => Some(out)
      case _ => None
    }

    def extractWithExtras: Option[(A, SArray[Byte])] = this match {
      case ParseComplete(out, bytes) => Some((out, bytes))
      case _ => None
    }
  }
  final case class ParseComplete[A](value: A, rest: SArray[Byte]) extends RespParserResult[A]
  final case class ParseIncomplete(arr: SArray[Byte]) extends RespParserResult[Nothing]
  final case class ParseError(message: String, cause: Option[Throwable]) extends RedisError with RespParserResult[Nothing]
  private[Resp] val CR = '\r'.toByte
  private[Resp] val LF = '\n'.toByte
  private[Resp] val Plus = '+'.toByte
  private[Resp] val Minus = '-'.toByte
  private[Resp] val Colon = ':'.toByte
  private[Resp] val Dollar = '$'.toByte
  private[Resp] val Star = '*'.toByte

  private[Resp] val MinusOne = "-1".getBytes()

  private[Resp] val CRLF = "\r\n".getBytes

  def renderRequest(nel: NonEmptyList[String]): Resp = {
    Resp.Array(Some(
      nel.toList.map(renderArg)
    ))
  }

  def renderArg(arg: String): Resp = {
    Resp.BulkString(Some(ByteVector.encodeString(arg)(StandardCharsets.UTF_8).fold(throw _ , identity)))
  }

  def encode(resp: Resp): SArray[Byte] = {
    resp match {
      case s@SimpleString(_) => SimpleString.encode(s)
      case e@Error(_) => Error.encode(e)
      case i@Integer(_) => Integer.encode(i)
      case b@BulkString(_) => BulkString.encode(b)
      case a@Array(_) => Array.encode(a)
    }
  }

  def toStringProtocol(resp: Resp)(implicit C: Charset = StandardCharsets.UTF_8) = 
    new String(encode(resp), C)

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

  def parseAll(arr: SArray[Byte]): RespParserResult[List[Resp]] = { // TODO Investigate Performance Benchmarks with Chain
    val listBuffer = new mutable.ListBuffer[Resp]
    def loop(arr: SArray[Byte]): RespParserResult[List[Resp]] = {
      if (arr.isEmpty) ParseComplete(listBuffer.toList, arr)
      else parse(arr) match {
        case ParseIncomplete(out) => 
          ParseComplete(listBuffer.toList, out) // Current Good Out, and partial remaining
        case ParseComplete(value, rest) =>
          listBuffer.append(value)
          loop(rest)
        case e@ParseError(_,_) => e
      }
    }
    loop(arr)
  }

  def parse(arr: SArray[Byte]): RespParserResult[Resp] = {
    if (arr.size > 0) {
      val switchVal = arr(0)
      switchVal match {
        case Plus => SimpleString.parse(arr)
        case Minus => Error.parse(arr)
        case Colon => Integer.parse(arr)
        case Dollar => BulkString.parse(arr)
        case Star => Array.parse(arr)
        case _   => ParseError(s"Resp.parse provided array does not begin with any of the valid bytes +,-,:,$$,* \n${ByteVector.view(arr).take(20).decodeUtf8}", None)
      }
    } else {
      ParseIncomplete(arr)
    }
  }

  object CodecUtils {
      private val asciiInt: Codec[scala.Int] = ascii.xmap(_.toInt, _.toString)
  private val crlf = BitVector('\r', '\n')
  private val delimInt: Codec[scala.Int] = crlfTerm(asciiInt)

  lazy val codec: Codec[RESP] =
    discriminated[RESP].by(byte)
      .typecase('+', crlfTerm(utf8).as[String.Simple])
      .typecase('-', crlfTerm(utf8).as[String.Error])
      .typecase(':', delimInt.as[Int])
      .typecase('$', bulk0)
      .typecase('*', array0)

  private lazy val bulk0: Codec[String.Bulk] =
    discriminated[String.Bulk].by(recover(constant('-')))
      .typecase(true, constant('1', '\r', '\n').xmap[String.Bulk.Nil.type](_ => String.Bulk.Nil, _ => ()))
      .typecase(false, (variableSizeBytes(delimInt, bytes) <~ constant(crlf)).as[String.Bulk.Full])

  lazy val array: Codec[Array] = /*logToStdOut(*/constant('*') ~> array0/*)*/

  private lazy val array0: Codec[Array] =
    discriminated[Array].by(recover(constant('-')))
      .typecase(true, constant('1', '\r', '\n').xmap[Array.Nil.type](_ => Array.Nil, _ => ()))
      .typecase(false, listOfN(delimInt, lazily(codec)).as[Array.Full])

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

        val (front, back) = bytes.splitAt(i)
        // println(s"bits = $bits; bits.decodeAscii = ${bits.decodeAscii}; front = ${front.decodeAscii}; i = $i")

        inner.decode(front.bits).map(_.copy(remainder = back.drop(2).bits))
      })
  }

    // First Byte is +
    // +foo/r/n
  case class SimpleString(value: String) extends Resp
  object SimpleString {
    def encode(s: SimpleString): SArray[Byte] = {
      val sA = s.value.getBytes(StandardCharsets.UTF_8)
      val buffer = mutable.ArrayBuilder.make[Byte]
      buffer.sizeHint(sA.size + 3)
      buffer.+=(Plus)
      buffer.++=(sA)
      buffer.++=(CRLF)
      buffer.result()
    }
    def parse(arr: SArray[Byte]): RespParserResult[SimpleString] = {
      var idx = 1
      try {
        if (arr(0) != Plus) throw ParseError("RespSimple String did not begin with +", None)
        while (idx < arr.size && arr(idx) != CR){
          idx += 1
        }
        if (idx < arr.size && (idx +1 < arr.size) && arr(idx +1) == LF){
          val out = new  String(arr, 1, idx - 1, StandardCharsets.UTF_8)
          ParseComplete(SimpleString(out), arr.drop(idx + 2))
        } else {
          ParseIncomplete(arr)
        }
      } catch {
        case NonFatal(e) => 
          ParseError(s"Error in RespSimpleString Processing: ${e.getMessage} - ${ByteVector.view(arr)}", Some(e))
      }
    }
  }
  // First Byte is -
  case class Error(value: String) extends RedisError with Resp{
    def message: String = s"Resp Error- $value"
    val cause: Option[Throwable] = None
  }
  object Error {
    def encode(error: Error): SArray[Byte] = 
      SArray(Minus) ++ error.value.getBytes(StandardCharsets.UTF_8) ++ CRLF
    def parse(arr: SArray[Byte]): RespParserResult[Error] = {
      var idx = 1
      try {
        if (arr(0) != Minus) throw ParseError("RespError did not begin with -", None)
        while (idx < arr.size && arr(idx) != CR){
          idx += 1
        }
        if (idx < arr.size && (idx +1 < arr.size) && arr(idx +1) == LF){
          val out = new  String(arr, 1, idx - 1, StandardCharsets.UTF_8)
          ParseComplete(Error(out), arr.drop(idx + 2))
        } else {
          ParseIncomplete(arr)
        }
      } catch {
        case NonFatal(e) => 
          ParseError(s"Error in Resp Error Processing: ${e.getMessage} - ${ByteVector.view(arr)}", Some(e))
      }
    }
  }
  // First Byte is :
  case class Integer(long: Long) extends Resp
  object Integer {
    def encode(i: Integer): SArray[Byte] = {
      SArray(Colon) ++ i.long.toString().getBytes(StandardCharsets.UTF_8) ++ CRLF
    }
    def parse(arr: SArray[Byte]): RespParserResult[Integer] = {
      var idx = 1
      try {
        if (arr(0) != Colon) throw ParseError("RespInteger String did not begin with :", None)
        while (idx < arr.size && arr(idx) != CR){
          idx += 1
        }
        if (idx < arr.size && (idx +1 < arr.size) && arr(idx +1) == LF){
          val out = new  String(arr, 1, idx - 1, StandardCharsets.UTF_8).toLong
          ParseComplete(Integer(out), arr.drop(idx + 2))
        } else {
          ParseIncomplete(arr)
        }
      } catch {
        case NonFatal(e) => 
          ParseError(s"Error in  RespInteger Processing: ${e.getMessage} - ${ByteVector.view(arr)}", Some(e))
      }
    }
  }
  // First Byte is $
  // $3/r/n/foo/r/n
  case class BulkString(value: Option[ByteVector]) extends Resp
  object BulkString {
    private val empty = SArray(Dollar) ++ MinusOne ++ CRLF
    def encode(b: BulkString): SArray[Byte] = {
      b.value match {
        case None => empty
        case Some(s) => {
          val bytes = s.toArray
          val size = bytes.size.toString.getBytes(StandardCharsets.UTF_8)
          val buffer = mutable.ArrayBuilder.make[Byte]
          buffer.+=(Dollar)
          buffer.++=(size)
          buffer.++=(CRLF)
          buffer.++=(bytes)
          buffer.++=(CRLF)
          buffer.result()
        }
      }
    }
    def parse(arr: SArray[Byte]): RespParserResult[BulkString] = {
      var idx = 1
      var length = -1
      try {
        if (arr(0) != Dollar) throw ParseError("RespBulkString String did not begin with +", None)
        while (idx < arr.size && arr(idx) != CR){
          idx += 1
        }
        if (idx < arr.size && (idx +1 < arr.size) && arr(idx +1) == LF){
          val out = new  String(arr, 1, idx - 1, StandardCharsets.UTF_8).toInt 
          length = out
          idx += 2
        }
        if (length == -1) ParseComplete(BulkString(None), arr.drop(idx))
        else if (idx + length + 2 <= arr.size)  {
          val out = ByteVector.view(arr, idx, length)
          ParseComplete(BulkString(Some(out)), arr.drop(idx + length + 2))
        } else ParseIncomplete(arr)
      } catch {
        case NonFatal(e) => 
          ParseError(s"Error in BulkString Processing: ${e.getMessage} - ${ByteVector.view(arr)}", Some(e))
      }
    }

  }
  // First Byte is *
  case class Array(a: Option[List[Resp]]) extends Resp
  object Array {
    def encode(a: Array): SArray[Byte] = {
      val buffer = mutable.ArrayBuilder.make[Byte]
      buffer += Star
      a.a match {
        case None => 
          buffer ++= MinusOne
          buffer ++= CRLF
        case Some(value) => 
          buffer ++= value.size.toString().getBytes(StandardCharsets.UTF_8)
          buffer ++= CRLF
          value.foreach(resp => 
            buffer ++= Resp.encode(resp)
          )
      }
      buffer.result()
    }
    def parse(arr: SArray[Byte]): RespParserResult[Array] = {
      var idx = 1
      var length = -1
      var sizeComplete = false
      try {
        if (arr(0) != Star) throw ParseError("RespArray String did not begin with *", None)
        while (idx < arr.size && arr(idx) != CR){
          idx += 1
        }
        if (idx < arr.size && (idx +1 < arr.size) && arr(idx +1) == LF){
          val out = new String(arr, 1, idx - 1, StandardCharsets.UTF_8).toInt 
          length = out
          idx += 2
          sizeComplete = true
        }
        if (!sizeComplete) ParseIncomplete(arr)
        else if (length == -1) ParseComplete(Array(None), arr.clone().drop(idx))
        else {
          @scala.annotation.tailrec
          def repeatParse(arr: SArray[Byte], decrease: Int, accum: List[Resp]) : RespParserResult[Array] = {
            if (decrease == 0) 
              ParseComplete(Array(Some(accum.reverse)), arr)
            else 
              Resp.parse(arr) match {
                case i@ParseIncomplete(_) => i
                case ParseComplete(value, rest) => repeatParse(rest, decrease - 1, value :: accum)
                case e@ParseError(_,_) => e
              }
          }
          // println("In Repeat Parse")
          val next = arr.clone().drop(idx)
          repeatParse(next, length, List.empty)
        }
      } catch {
        case NonFatal(e) => 
          ParseError(s"Error in RespArray Processing: ${e.getMessage} - ${ByteVector.view(arr)}", Some(e))
      }
    }
  }
}