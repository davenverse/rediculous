package io.chrisdavenport.rediculous

import cats.syntax.all._
import io.chrisdavenport.rediculous.Resp.ParseComplete
import org.scalacheck.Prop
import scodec.bits._
import RespArbitraries._
import org.scalacheck.Arbitrary
import io.chrisdavenport.rediculous.Resp.ParseComplete
import io.chrisdavenport.rediculous.Resp.ParseIncomplete
import io.chrisdavenport.rediculous.Resp.ParseError
import scodec.Attempt.Successful
import scodec.Attempt
import scodec.Attempt.Failure

class RespSpec extends munit.ScalaCheckSuite {

  def toBV(s: String): ByteVector = ByteVector.encodeUtf8(s).fold(throw _, identity(_))
  def toString(bv: ByteVector): String = bv.decodeUtf8.fold(throw _, identity(_))

  // // "Resp" should {
  //   test("Resp parse a simple-string") {
  //     Resp.SimpleString.parse("+OK\r\n".getBytes()) match {
  //       case ParseComplete(value, _) => 
  //         assert(value.value === "OK")
  //       case _ => fail("Did not complete")
  //     }
  //   }

  //   // Figure out how to constrain
  //   property("round-trip a simple-string"){ Prop.forAll { (init: Resp.SimpleString) =>
  //     // val initS = s.replace("\r", "").replace("\n", "") // simple strings cannot contain new lines
  //     // val init = Resp.SimpleString(s)
  //     Resp.SimpleString.parse(
  //       Resp.SimpleString.encode(init)
  //     ) match {
  //       case ParseComplete(value, rest) => 
  //         assertEquals(value, init)
  //         assertEquals(rest.size, 0)
  //         // (value must_=== init).and(
  //         //   rest must beEmpty
  //         // )
  //       case o => fail(s"Unexpected: $o")
  //     }
  //   }}

  //   test("Resp parse an error"){
  //     val s = "-Error message\r\n"
  //     Resp.Error.parse(s.getBytes()) match {
  //       case ParseComplete(value, _) => 
  //         assert(value.value === "Error message")
  //       case o => fail(s"Unexpected: $o")
  //     }
  //   }

  //   // Figure out how to constraint
  //   property("round-trip an error"){Prop.forAll{(init : Resp.Error) => {
  //     Resp.Error.parse(
  //       Resp.Error.encode(init)
  //     ) match {
  //       case ParseComplete(value, rest) => 
  //         assert(value.value === init.value && rest.isEmpty)
  //       case o => fail(s"Unexpected $o")
  //     }
  //   }}}

  //   test("Resp parse an integer") {
  //     val s = ":1000\r\n"
  //     Resp.Integer.parse(s.getBytes()) match {
  //       case ParseComplete(value, _) => 
  //         assert(value.long === 1000L)
  //       case o => fail(s"Unexpected $o")
  //     }
  //   }

  //   test("Resp encode an integer") {
  //     val x = 8971392965300387418L
  //     val arr = Resp.Integer.encode(Resp.Integer(x))
  //     val s = new String(arr)
  //     assert(s === ":8971392965300387418\r\n")
  //   }

  //   property("Resp round-trip an integer"){
  //     Prop.forAll{ (init: Resp.Integer) => 
  //       // val init = Resp.Integer(l)
  //       Resp.Integer.parse(
  //         Resp.Integer.encode(init)
  //       ).extractWithExtras match {
  //         case Some((value, rest)) => 
  //           assertEquals(value, init)
  //           assertEquals(rest.size, 0, "Parse Should Leave No Extra Bytes")
  //         case o => fail(s"Got Unexpected Result $o")
  //       }
  //     }
  //   }

  //   test("Resp parse a bulk string") {
  //     val s = "$6\r\nfoobar\r\n"
  //     Resp.BulkString.parse(s.getBytes()) match {
  //       case ParseComplete(value, _) => 
  //       value.value match {
  //         case Some(s2) => assert(s2 == toBV("foobar"))
  //         case None => fail("None")
  //       }
  //       case o => fail(s"Unexpected $o")
  //     }
  //   }

  //   test("Resp parse an empty bulk string") {
  //     val s = "$0\r\n\r\n"
  //     Resp.BulkString.parse(s.getBytes()) match {
  //       case ParseComplete(value, _) => 
  //       value.value match {
  //         case Some(s2) => assert(s2 == toBV(""))
  //         case None => fail("Empty Value")
  //       }
  //       case o => fail(s"Unexpected $o")
  //     }
  //   }

  //   test("Resp parse a null bulk string") {
  //     val s = "$-1\r\n"
  //     Resp.BulkString.parse(s.getBytes()) match {
  //       case ParseComplete(value, _) => 
  //         assert(value.value == None)
  //       case o => fail(s"Unexpected $o")
  //     }
  //   }

  //   property("Resp round-trip a bulk-string"){ Prop.forAll{ (init: Resp.BulkString) => 
  //     // val init = Resp.BulkString(Some(toBV(s)))
  //     Resp.BulkString.parse(
  //       Resp.BulkString.encode(init)
  //     ).extractWithExtras match {
  //       case Some((value, extra)) => 
  //         assertEquals(value, init)
  //         assertEquals(extra.size, 0)
  //       case None => fail(s"failed with input $init")
  //     }}
  //   }

  //   test("Resp parse an empty array") {
  //     val init = "*0\r\n"
  //     Resp.Array.parse(init.getBytes()) match {
  //       case ParseComplete(value, _) => 
  //         value.a match {
  //           case Some(l) => assert (l.isEmpty)
  //           case None => fail("Array Returned None")
  //         }
  //       case o => fail(s"Unexpected $o")
  //     }
  //   }

  //   test("Resp parse an array of bulk strings") {
  //     val init = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
  //     Resp.Array.parse(init.getBytes()) match {
  //       case ParseComplete(value, _) => 
  //         value.a match {
  //           case (Some(Resp.BulkString(Some(foo)) :: Resp.BulkString(Some(bar)) :: Nil)) if foo == toBV("foo") && bar == toBV("bar") => assert(true)
  //           case otherwise => fail(s"Incorrect Output $otherwise")
  //         }
  //       case o => fail(s"Unexpected $o")
  //     }
  //   }

  //   test("Resp parse an array of integers") {
  //     val init = "*3\r\n:1\r\n:2\r\n:3\r\n"
  //     Resp.Array.parse(init.getBytes()) match {
  //       case ParseComplete(value, _) => 
  //         value.a match {
  //           case Some(l) => 
  //             assertEquals(l, List(Resp.Integer(1L), Resp.Integer(2L), Resp.Integer(3L)))
  //           case None => fail("Incorrect Value")
  //         }
  //       case o => fail(s"Unexpected $o")
  //     }
  //   }

  //   test("Resp parse an array of mixed types") {
  //     val init = "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n"
  //     Resp.Array.parse(init.getBytes()) match {
  //       case ParseComplete(value, _) => 
  //         value.a match {
  //           case Some(l) => 
  //             assertEquals(l, List(Resp.Integer(1L), Resp.Integer(2L), Resp.Integer(3L), Resp.Integer(4L), Resp.BulkString(Some(toBV("foobar")))))
  //           case None => fail("Value Empty")
  //         }
  //       case o => fail(s"Unexpected $o")
  //     }
  //   }

  //   test("Resp parse a null array") {
  //     val init = "*-1\r\n"
  //     Resp.Array.parse(init.getBytes()) match {
  //       case ParseComplete(value, _) => 
  //         assertEquals(value.a, None)
  //       case o => fail(s"Unexpected $o")
  //     }
  //   }

  //   test("Resp parse empty array") {
  //     val init = "*0\r\n"
  //     Resp.Array.parse(init.getBytes()) match {
  //       case ParseComplete(value, rest) => 
  //         assertEquals(value, Resp.Array(Some(List.empty)))
  //         assertEquals(rest.size, 0)
  //       case o => fail(s"Unexpected $o")
  //       // case ParseIncomplete(arr) =>
  //       // case ParseError(message, cause) =>
  //     }
  //   }

  //   test("Resp parse an array of arrays") {
  //     val init = "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n"
  //     Resp.Array.parse(init.getBytes()).extract match {
  //       case Some(value) => 
  //         val expected = Resp.Array(Some(
  //           List(
  //             Resp.Array(Some(List(Resp.Integer(1), Resp.Integer(2), Resp.Integer(3)))),
  //             Resp.Array(Some(List(Resp.SimpleString("Foo"), Resp.Error("Bar"))))
  //           )
  //         ))
  //         assertEquals(value, expected)
  //       case o => fail(s"Unexpected $o")
  //     }
  //   }

  // property("Resp round-trip an array"){ Prop.forAll{ (init: Resp.Array) => 
  //     Resp.Array.parse(
  //       Resp.Array.encode(init)
  //     ).extractWithExtras match {
  //       case Some((value, rest)) => 
  //         assertEquals(value, init)
  //         assertEquals(rest.size, 0)
  //       case None => fail(s"failed with input $init")
  //     }}
  //   }

  // property("Resp round-trip"){ Prop.forAll{(init: Resp) => 
  //   Resp.parse(
  //     Resp.encode(init)
  //   ).extractWithExtras match {
  //     case Some((value, rest)) => 
  //       assertEquals(value, init)
  //       assertEquals(rest.size, 0)
  //     case None => fail(s"failed with input $init")
  //   }
  // }}


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

  // test("Round trip the nil bulk string"){
  //   val init = Resp.BulkString(None)
  //   val bv = ByteVector.view(Resp.toStringProtocol(init).getBytes())
  //   // println(bv.decodeAscii)
  //   // "*3\r\n-r*$2  \r\n"
  //   Resp.CodecUtils.codec.decode(bv.bits) match {
  //     case Successful(value) => 
  //       assertEquals(value.value, init)
  //       assertEquals(value.remainder.size, 0L)
  //     case Attempt.Failure(err) =>
  //       val base = Resp.CodecUtils.codec.encode(init)
  //       assert(false, s"Uh-oh, ${err} - $base")
  //   }
  // }

  test("Round Trip This Specific Array"){
    val elem1 = Resp.BulkString(None)
    // val elem2 = Resp.Error("r*")
    val elem3 = Resp.BulkString(Some(ByteVector[Byte](0, 0)))
    val elem2 =  Resp.SimpleString("foo")
    val init = Resp.Array(Some(List(elem1, elem2)))
    val bv = ByteVector.view(Resp.toStringProtocol(init).getBytes())
    // println(bv.decodeAscii)
    // "*3\r\n-r*$2  \r\n"
    Resp.CodecUtils.codec.decode(bv.bits) match {
      case Successful(value) => 
        assertEquals(value.value, init)
        assertEquals(value.remainder.size, 0L)
      case Attempt.Failure(err) =>
        val base = Resp.CodecUtils.codec.encode(init)
        assert(false, s"Uh-oh, ${err} - $base")
    }
  }

  private def flatEncode(s: String): String = s.replace("\r", "\\r").replace("\n", "\\n")

  // test("Encode like old"){
  //   val elem1 = Resp.BulkString(None)
  //   // val elem2 = Resp.Error("r*")
  //   // val elem3 = Resp.BulkString(Some(ByteVector[Byte](0, 0)))
  //   val init = Resp.Array(Some(List(elem1, Resp.SimpleString("foo"))))
  //   val bv = ByteVector.view(Resp.toStringProtocol(init).getBytes())
  //   Resp.CodecUtils.codec.encode(init) match {
  //     case Failure(cause) => 

  //     case Successful(value) => 
  //       // def 
  //       assertEquals(value.bytes.decodeAscii.map(flatEncode).fold(throw _, identity(_)), Resp.toStringProtocol(init))
  //       // assertEquals(value.bytes, bv)

  //   }
  // }

  // case class PosInt(i: Int)
  // object PosInt {
  //   implicit val arb: Arbitrary[PosInt] = org.scalacheck.Arbitrary(
  //     org.scalacheck.Gen.chooseNum(1, 2000).map(PosInt(_))
  //   )
  // }

  // property("Incomplete BulkString anything else"){ Prop.forAll{
  //   (init: Resp.BulkString, i: PosInt) => 
  //     val cli = Resp.toStringRedisCLI(init)

  //     val encoded = Resp.encode(init)
  //     val atleast1 = Math.min(encoded.size - 1, i.i)
  //     val out = encoded.dropRight(atleast1)
  //     val encodedBV = ByteVector(out.clone())
      
  //     Resp.BulkString.parse(
  //       out
  //     ) match {
  //       case Resp.ParseIncomplete(rest) => 
  //         assertEquals(ByteVector(rest).decodeUtf8.map(_.replace("\r", """\r""").replace("\n", """\n""")), encodedBV.decodeUtf8.map(_.replace("\r", """\r""").replace("\n", """\n""")))
  //       case _ => fail(s"failed with input $init - $i\nResp:$cli")
  //     }
  // }}
  
}