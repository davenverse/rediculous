package io.chrisdavenport.rediculous

import cats.syntax.all._
import org.scalacheck.Prop
import scodec.bits._
import RespArbitraries._
import org.scalacheck.Arbitrary
import scodec.Attempt.Successful
import scodec.Attempt
import scodec.Attempt.Failure

class RespSpec extends munit.ScalaCheckSuite {

  def toBV(s: String): ByteVector = ByteVector.encodeUtf8(s).fold(throw _, identity(_))
  def toString(bv: ByteVector): String = bv.decodeUtf8.fold(throw _, identity(_))

  def testDecode(init: String, expected: Resp) = {
    Resp.CodecUtils.codec.decode(ByteVector(init.getBytes()).bits).toOption match {
      case Some(value) =>
        assertEquals(value.value, expected)
        assertEquals(value.remainder.size, 0L)
      case None => fail("Did Not get successful")
    }
  }

  private def flatEncode(s: String): String = s.replace("\r", "\\r").replace("\n", "\\n")

  test("Resp parse a simple-string") {
    testDecode("+OK\r\n", Resp.SimpleString("OK"))
  }

  test("Resp parse an error"){
    val s = "-Error message\r\n"
    testDecode(s, Resp.Error("Error message"))
  }

  test("Resp parse an integer") {
    val s = ":1000\r\n"
    testDecode(s, Resp.Integer(1000L))
  }

  test("Resp encode an integer") {
    val x = 8971392965300387418L
    val arr = Resp.CodecUtils.codec.encode(Resp.Integer(x)).toOption.flatMap(b => 
      b.bytes.decodeAscii.toOption
    )
    arr match {
      case Some(value) => 
        assertEquals(value, ":8971392965300387418\r\n")
      case None => fail("Did not encode correct")
    }
  }

  test("Resp parse a bulk string") {
    val s = "$6\r\nfoobar\r\n"
    val expected = Resp.BulkString(Some(toBV("foobar")))
    testDecode(s, expected)
  }

  test("Resp parse an empty bulk string") {
    val s = "$0\r\n\r\n"
    val expected = Resp.BulkString(Some(ByteVector.empty))
    testDecode(s, expected)
  }

  test("Resp parse a null bulk string") {
    val s = "$-1\r\n"
    val expected = Resp.BulkString(None)
    testDecode(s, expected)
  }

  test("Resp parse an empty array") {
    val init = "*0\r\n"
    val expected = Resp.Array(Some(List.empty))
    testDecode(init, expected)
  }

  test("Resp parse an array of bulk strings") {
    val init = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
    val expected = Resp.Array(Some(
      Resp.BulkString(Some(toBV("foo"))) :: Resp.BulkString(Some(toBV("bar"))) :: Nil
    ))
    testDecode(init, expected)
  }

  test("Resp parse an array of integers") {
    val init = "*3\r\n:1\r\n:2\r\n:3\r\n"
    val expected = Resp.Array(Some(
      List(Resp.Integer(1L), Resp.Integer(2L), Resp.Integer(3L))
    ))
    testDecode(init, expected)
  }

  test("Resp parse an array of mixed types") {
    val init = "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n"
    val expected = Resp.Array(Some(
      List(Resp.Integer(1L), Resp.Integer(2L), Resp.Integer(3L), Resp.Integer(4L), Resp.BulkString(Some(toBV("foobar"))))
    ))
    testDecode(init, expected)
  }

  test("Resp parse a null array") {
    val init = "*-1\r\n"
    val expected = Resp.Array(None)
    testDecode(init, expected)
  }

  test("Resp parse empty array") {
    val init = "*0\r\n"
    val expected = Resp.Array(Some(List.empty))
    testDecode(init, expected)
  }

  test("Resp parse an array of arrays") {
    val init = "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n"
    Resp.CodecUtils.codec.decode(ByteVector(init.getBytes()).bits).toOption match {
      case Some(value) => 
        val expected: Resp = Resp.Array(Some(
          List(
            Resp.Array(Some(List(Resp.Integer(1), Resp.Integer(2), Resp.Integer(3)))),
            Resp.Array(Some(List(Resp.SimpleString("Foo"), Resp.Error("Bar"))))
          )
        ))
        assertEquals(value.value, expected)
      case o => fail(s"Unexpected $o")
    }
  }


  property("Resp round-trip - BulkString"){ Prop.forAll{(init: Resp.BulkString) => 
    Resp.CodecUtils.codec.encode(init).flatMap(Resp.CodecUtils.codec.decode(_)) match {
      case Successful(value) => 
        assertEquals(value.value, init)
        assertEquals(value.remainder.size, 0L)
      case Attempt.Failure(err) =>
        val base = Resp.CodecUtils.codec.encode(init)
        assert(false, s"Uh-oh, ${err} - $base")
    }
  }}

  property("Resp round-trip - SimpleString"){ Prop.forAll{(init: Resp.SimpleString) => 
    Resp.CodecUtils.codec.encode(init).flatMap(Resp.CodecUtils.codec.decode(_)) match {
      case Successful(value) => 
        assertEquals(value.value, init)
        assertEquals(value.remainder.size, 0L)
      case Attempt.Failure(err) =>
        val base = Resp.CodecUtils.codec.encode(init)
        assert(false, s"Uh-oh, ${err} - $base")
    }
  }}

  property("Resp round-trip - Error"){ Prop.forAll{(init: Resp.Error) => 
    Resp.CodecUtils.codec.encode(init).flatMap(Resp.CodecUtils.codec.decode(_)) match {
      case Successful(value) => 
        assertEquals(value.value, init)
        assertEquals(value.remainder.size, 0L)
      case Attempt.Failure(err) =>
        val base = Resp.CodecUtils.codec.encode(init)
        assert(false, s"Uh-oh, ${err} - $base")
    }
  }}

  property("Resp round-trip - Integer"){ Prop.forAll{(init: Resp.Integer) => 
    Resp.CodecUtils.codec.encode(init).flatMap(Resp.CodecUtils.codec.decode(_)) match {
      case Successful(value) => 
        assertEquals(value.value, init)
        assertEquals(value.remainder.size, 0L)
      case Attempt.Failure(err) =>
        val base = Resp.CodecUtils.codec.encode(init)
        assert(false, s"Uh-oh, ${err} - $base")
    }
  }}


  property("Resp round-trip - Array"){ Prop.forAll{(init: Resp.Array) => 
    Resp.CodecUtils.codec.encode(init).flatMap(Resp.CodecUtils.codec.decode(_)) match {
      case Successful(value) => 
        assertEquals(value.value, init)
        assertEquals(value.remainder.size, 0L)
      case Attempt.Failure(err) =>
        val base = Resp.CodecUtils.codec.encode(init)
        assert(false, s"Uh-oh, ${err} - $base")
    }
  }}

  property("Resp round-trip"){ Prop.forAll{(init: Resp) => 
    Resp.CodecUtils.codec.encode(init).flatMap(Resp.CodecUtils.codec.decode(_)) match {
      case Successful(value) => 
        assertEquals(value.value, init)
        assertEquals(value.remainder.size, 0L)
      case Attempt.Failure(err) =>
        val base = Resp.CodecUtils.codec.encode(init)
        assert(false, s"Uh-oh, ${err} - $base")
    }
  }}

  test("Round Trip This Specific Array"){
    val elem1 = Resp.BulkString(None)
    val elem3 = Resp.BulkString(Some(ByteVector[Byte](0, 0)))
    val elem2 =  Resp.SimpleString("foo")
    val init = Resp.Array(Some(List(elem1, elem2, elem3)))
    val bv = ByteVector.view(Resp.toStringProtocol(init).getBytes())
    Resp.CodecUtils.codec.decode(bv.bits) match {
      case Successful(value) => 
        assertEquals(value.value, init)
        assertEquals(value.remainder.size, 0L)
      case Attempt.Failure(err) =>
        val base = Resp.CodecUtils.codec.encode(init)
        assert(false, s"Uh-oh, ${err} - $base")
    }
  }

  
  
}