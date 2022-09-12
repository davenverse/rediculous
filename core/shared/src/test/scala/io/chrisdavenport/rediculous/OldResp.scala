package io.chrisdavenport.rediculous


import scala.collection.mutable
import cats.data.NonEmptyList
import cats.implicits._
import scala.util.control.NonFatal
import java.nio.charset.StandardCharsets
import java.nio.charset.Charset
import scodec.bits.ByteVector


object OldResp {
  import scala.{Array => SArray}
  import Resp._
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
  private[OldResp] val CR = '\r'.toByte
  private[OldResp] val LF = '\n'.toByte
  private val Plus = '+'.toByte
  private val Minus = '-'.toByte
  private val Colon = ':'.toByte
  private val Dollar = '$'.toByte
  private val Star = '*'.toByte

  private val MinusOne = "-1".getBytes()

  private val CRLF = "\r\n".getBytes

  def encode(resp: Resp): SArray[Byte] = {
    resp match {
      case s@Resp.SimpleString(_) => SimpleString.encode(s)
      case e@Resp.Error(_) => Error.encode(e)
      case i@Resp.Integer(_) => Integer.encode(i)
      case b@Resp.BulkString(_) => BulkString.encode(b)
      case a@Resp.Array(_) => Array.encode(a)
    }
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
          ParseComplete(Resp.SimpleString(out), arr.drop(idx + 2))
        } else {
          ParseIncomplete(arr)
        }
      } catch {
        case NonFatal(e) => 
          ParseError(s"Error in RespSimpleString Processing: ${e.getMessage} - ${ByteVector.view(arr)}", Some(e))
      }
    }
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
          ParseComplete(Resp.Error(out), arr.drop(idx + 2))
        } else {
          ParseIncomplete(arr)
        }
      } catch {
        case NonFatal(e) => 
          ParseError(s"Error in Resp Error Processing: ${e.getMessage} - ${ByteVector.view(arr)}", Some(e))
      }
    }
  }
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
          ParseComplete(Resp.Integer(out), arr.drop(idx + 2))
        } else {
          ParseIncomplete(arr)
        }
      } catch {
        case NonFatal(e) => 
          ParseError(s"Error in  RespInteger Processing: ${e.getMessage} - ${ByteVector.view(arr)}", Some(e))
      }
    }
  }

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
        if (length == -1) ParseComplete(Resp.BulkString(None), arr.drop(idx))
        else if (idx + length + 2 <= arr.size)  {
          val out = ByteVector.view(arr, idx, length)
          ParseComplete(Resp.BulkString(Some(out)), arr.drop(idx + length + 2))
        } else ParseIncomplete(arr)
      } catch {
        case NonFatal(e) => 
          ParseError(s"Error in BulkString Processing: ${e.getMessage} - ${ByteVector.view(arr)}", Some(e))
      }
    }

  }

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
            buffer ++= OldResp.encode(resp)
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
        else if (length == -1) ParseComplete(Resp.Array(None), arr.clone().drop(idx))
        else {
          @scala.annotation.tailrec
          def repeatParse(arr: SArray[Byte], decrease: Int, accum: List[Resp]) : RespParserResult[Array] = {
            if (decrease == 0) 
              ParseComplete(Resp.Array(Some(accum.reverse)), arr)
            else 
              OldResp.parse(arr) match {
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