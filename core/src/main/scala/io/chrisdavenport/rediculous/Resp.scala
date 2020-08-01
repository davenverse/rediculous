package io.chrisdavenport.rediculous

import fs2._
import scala.collection.mutable
import cats.implicits._

sealed trait Resp

object Resp {
  import scala.{Array => SArray}
  sealed trait RespParserResult[+A]
  final case class ParseComplete[A](value: A, rest: SArray[Byte]) extends RespParserResult[A]
  final case class ParseIncomplete(arr: SArray[Byte]) extends RespParserResult[Nothing]
  final case class ParseError(message: String, cause: Option[Throwable]) extends Throwable(message, cause.orNull) with RespParserResult[Nothing]
  private[Resp] val CR = '\r'.toByte
  private[Resp] val LF = '\n'.toByte
  private[Resp] val Plus = '+'.toByte
  private[Resp] val Minus = '-'.toByte
  private[Resp] val Colon = ':'.toByte
  private[Resp] val Dollar = '$'.toByte
  private[Resp] val Star = '*'.toByte

  private[Resp] val MinusOne = "-1".getBytes()

  private[Resp] val CRLF = "\r\n".getBytes

  def encode(resp: Resp): SArray[Byte] = {
    resp match {
      case s@SimpleString(_) => SimpleString.encode(s)
      case e@Error(_) => Error.encode(e)
      case i@Integer(_) => Integer.encode(i)
      case b@BulkString(_) => BulkString.encode(b)
      case a@Array(_) => Array.encode(a)
    }
  }

  def parseAll(arr: SArray[Byte]): RespParserResult[List[Resp]] = {
    val listBuffer = new mutable.ListBuffer[Resp]
    def loop(arr: SArray[Byte]): RespParserResult[List[Resp]] = {
      if (arr.isEmpty) ParseComplete(listBuffer.toList, arr)
      else parse(arr) match {
        case i@ParseIncomplete(out) => ParseComplete(listBuffer.toList, out)
        case ParseComplete(value, rest) =>
          listBuffer.append(value)
          loop(rest)
        case e@ParseError(_,_) => e
      }
    }
    loop(arr)
  }

  def parse(arr: SArray[Byte]): RespParserResult[Resp] = {
    if (arr.size >= 0) {
      val switchVal = arr(0)
      switchVal match {
        case Plus => SimpleString.parse(arr)
        case Minus => Error.parse(arr)
        case Colon => Integer.parse(arr)
        case Dollar => BulkString.parse(arr)
        case Star => Array.parse(arr)
        case _   => ParseError("Resp.parse provided array does not begin with any of the valid bytes +,-,:,$,*", None)
      }
    } else {
      ParseIncomplete(arr)
    }
  }

    // First Byte is +
  case class SimpleString(value: String) extends Resp
  object SimpleString {
    def encode(s: SimpleString): SArray[Byte] = {
      SArray(Plus) ++ s.value.getBytes() ++ CRLF
    }
    def parse(arr: SArray[Byte]): RespParserResult[SimpleString] = {
      var idx = 1
      try {
        if (arr(0) != Plus) throw ParseError("RespSimple String did not begin with +", None)
        while (idx < arr.size && arr(idx) != CR){
          idx += 1
        }
        if (idx < arr.size && (idx +1 < arr.size) && arr(idx +1) == LF){
          val out = new  String(arr, 1, idx - 1)
          ParseComplete(SimpleString(out), arr.drop(idx + 2))
        } else {
          ParseIncomplete(arr)
        }
      } catch {
        case scala.util.control.NonFatal(e) => 
          ParseError(s"Error in RespSimpleString Processing: ${e.getMessage}", Some(e))
      }
    }
  }
  // First Byte is -
  case class Error(value: String) extends Throwable(s"Resp Error- $value") with Resp
  object Error {
    def encode(error: Error): SArray[Byte] = 
      SArray(Minus) ++ error.value.getBytes() ++ CRLF
    def parse(arr: SArray[Byte]): RespParserResult[Error] = {
      var idx = 1
      try {
        if (arr(0) != Minus) throw ParseError("RespError did not begin with -", None)
        while (idx < arr.size && arr(idx) != CR){
          idx += 1
        }
        if (idx < arr.size && (idx +1 < arr.size) && arr(idx +1) == LF){
          val out = new  String(arr, 1, idx - 1)
          ParseComplete(Error(out), arr.drop(idx + 2))
        } else {
          ParseIncomplete(arr)
        }
      } catch {
        case scala.util.control.NonFatal(e) => 
          ParseError(s"Error in Resp Error Processing: ${e.getMessage}", Some(e))
      }
    }
  }
  // First Byte is :
  case class Integer(long: Long) extends Resp
  object Integer {
    def encode(i: Integer): SArray[Byte] = {
      SArray(Colon) ++ i.long.toString().getBytes().dropRight(1)
    }
    def parse(arr: SArray[Byte]): RespParserResult[Integer] = {
      var idx = 1
      try {
        if (arr(0) != Colon) throw ParseError("RespInteger String did not begin with :", None)
        while (idx < arr.size && arr(idx) != CR){
          idx += 1
        }
        if (idx < arr.size && (idx +1 < arr.size) && arr(idx +1) == LF){
          val out = new  String(arr, 1, idx - 1).toLong
          ParseComplete(Integer(out), arr.drop(idx + 2))
        } else {
          ParseIncomplete(arr)
        }
      } catch {
        case scala.util.control.NonFatal(e) => 
          ParseError(s"Error in  RespInteger Processing: ${e.getMessage}", Some(e))
      }
    }
  }
  // First Byte is $
  case class BulkString(value: Option[String]) extends Resp
  object BulkString {
    def encode(b: BulkString): SArray[Byte] = {
      b.value match {
        case None => SArray(Dollar) ++ MinusOne ++ CRLF
        case Some(s) => SArray(Dollar) ++ s.length().toString().getBytes() ++ CRLF ++ 
          s.getBytes() ++ CRLF
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
          val out = new  String(arr, 1, idx - 1).toInt 
          length = out
          idx += 2
        }
        if (length == -1) ParseComplete(BulkString(None), arr.drop(idx))
        else {
          val out = new String(arr, idx, length)
          ParseComplete(BulkString(Some(out)), arr.drop(idx + length + 2))
        }
      } catch {
        case scala.util.control.NonFatal(e) => 
          ParseError(s"Error in BulkString Processing: ${e.getMessage}", Some(e))
      }
    }

  }
  // First Byte is *
  case class Array(a: Option[List[Resp]]) extends Resp
  object Array {
    def encode(a: Array): SArray[Byte] = {
      val buffer = new mutable.ArrayBuffer[Byte]
      buffer.addOne(Star)
      a.a match {
        case None => 
          buffer.addAll(MinusOne)
          buffer.addAll(CRLF)
        case Some(value) => 
          buffer.addAll(value.size.toString().getBytes())
          buffer.addAll(CRLF)
          value.foreach(resp => 
            buffer.addAll(Resp.encode(resp))
          )
      }
      buffer.toArray
    }
    def parse(arr: SArray[Byte]): RespParserResult[Array] = {
      var idx = 1
      var length = -1
      try {
        if (arr(0) != Star) throw ParseError("RespArray String did not begin with *", None)
        while (idx < arr.size && arr(idx) != CR){
          idx += 1
        }
        if (idx < arr.size && (idx +1 <= arr.size) && arr(idx +1) == LF){
          val out = new  String(arr, 1, idx - 1).toInt 
          length = out
          idx += 2
        }
        if (length == -1) ParseComplete(Array(None), arr.drop(idx))
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
          val next = arr.drop(idx)
          repeatParse(next, length, List.empty)
        }
      } catch {
        case scala.util.control.NonFatal(e) => 
          ParseError(s"Error in RespArray Processing: ${e.getMessage}", Some(e))
      }
    }
  }
}