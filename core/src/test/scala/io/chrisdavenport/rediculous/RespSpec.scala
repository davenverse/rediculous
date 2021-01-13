package io.chrisdavenport.rediculous

import cats.syntax.all._
import io.chrisdavenport.rediculous.Resp.ParseComplete
import org.scalacheck.Prop

class RespSpec extends munit.ScalaCheckSuite {
  // "Resp" should {
    test("Resp parse a simple-string") {
      Resp.SimpleString.parse("+OK\r\n".getBytes()) match {
        case ParseComplete(value, _) => 
          assert(value.value === "OK")
        case _ => fail("Did not complete")
      }
    }

    // Figure out how to constrain
    // property("round-trip a simple-string"){ Prop.forAll { (s: String) => (!s.contains("\r") && !s.contains("\n")) ==> {
    //   // val initS = s.replace("\r", "").replace("\n", "") // simple strings cannot contain new lines
    //   val init = Resp.SimpleString(s)
    //   Resp.SimpleString.parse(
    //     Resp.SimpleString.encode(init)
    //   ) match {
    //     case ParseComplete(value, rest) => 
    //       (value must_=== init).and(
    //         rest must beEmpty
    //       )
    //     case o => ko(s"Unexpected: $o")
    //   }
    // }}}

    test("Resp parse an error"){
      val s = "-Error message\r\n"
      Resp.Error.parse(s.getBytes()) match {
        case ParseComplete(value, _) => 
          assert(value.value === "Error message")
        case o => fail(s"Unexpected: $o")
      }
    }

    // Figure out how to constraint
    // property("round-trip an error"){Prop.forAll{(s : String) =>  (!s.contains("\r") && !s.contains("\n")) ==> {
    //   val init = Resp.Error(s)
    //   Resp.Error.parse(
    //     Resp.Error.encode(init)
    //   ) match {
    //     case ParseComplete(value, rest) => 
    //       assert(value.value === init.value && rest.isEmpty)
    //     case o => fail(s"Unexpected $o")
    //   }
    // }}}

    test("Resp parse an integer") {
      val s = ":1000\r\n"
      Resp.Integer.parse(s.getBytes()) match {
        case ParseComplete(value, _) => 
          assert(value.long === 1000L)
        case o => fail(s"Unexpected $o")
      }
    }

    test("Resp encode an integer") {
      val x = 8971392965300387418L
      val arr = Resp.Integer.encode(Resp.Integer(x))
      val s = new String(arr)
      assert(s === ":8971392965300387418\r\n")
    }

    property("Resp round-trip an integer"){
      Prop.forAll{ (l: Long) => 
        val init = Resp.Integer(l)
        Resp.Integer.parse(
          Resp.Integer.encode(init)
        ).extract match {
          case Some(value) => 
            assertEquals(value, init)
          case o => fail(s"Got Unexpected Result $o")
        }
      }
    }

    test("Resp parse a bulk string") {
      val s = "$6\r\nfoobar\r\n"
      Resp.BulkString.parse(s.getBytes()) match {
        case ParseComplete(value, _) => 
        value.value match {
          case Some(s2) => assert(s2 === "foobar")
          case None => fail("None")
        }
        case o => fail(s"Unexpected $o")
      }
    }

    test("Resp parse an empty bulk string") {
      val s = "$0\r\n\r\n"
      Resp.BulkString.parse(s.getBytes()) match {
        case ParseComplete(value, _) => 
        value.value match {
          case Some(s2) => assert(s2 === "")
          case None => fail("Empty Value")
        }
        case o => fail(s"Unexpected $o")
      }
    }

    test("Resp parse a null bulk string") {
      val s = "$-1\r\n"
      Resp.BulkString.parse(s.getBytes()) match {
        case ParseComplete(value, _) => 
          assert(value.value === None)
        case o => fail(s"Unexpected $o")
      }
    }

    property("Resp round-trip a bulk-string"){ Prop.forAll{ (s : String) => 
      val init = Resp.BulkString(Some(s))
      Resp.BulkString.parse(
        Resp.BulkString.encode(init)
      ).extract match {
        case Some(value) => 
          assertEquals(value, init)

        case None => fail(s"failed with input $s")
      }}
    }

    test("Resp parse an empty array") {
      val init = "*0\r\n"
      Resp.Array.parse(init.getBytes()) match {
        case ParseComplete(value, _) => 
          value.a match {
            case Some(l) => assert (l.isEmpty)
            case None => fail("Array Returned None")
          }
        case o => fail(s"Unexpected $o")
      }
    }

    test("Resp parse an array of bulk strings") {
      val init = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
      Resp.Array.parse(init.getBytes()) match {
        case ParseComplete(value, _) => 
          value.a match {
            case Some(Resp.BulkString(Some("foo")) :: Resp.BulkString(Some("bar")) :: Nil) => assert(true)
            case otherwise => fail(s"Incorrect Output $otherwise")
          }
        case o => fail(s"Unexpected $o")
      }
    }

    test("Resp parse an array of integers") {
      val init = "*3\r\n:1\r\n:2\r\n:3\r\n"
      Resp.Array.parse(init.getBytes()) match {
        case ParseComplete(value, _) => 
          value.a match {
            case Some(l) => 
              assertEquals(l, List(Resp.Integer(1L), Resp.Integer(2L), Resp.Integer(3L)))
            case None => fail("Incorrect Value")
          }
        case o => fail(s"Unexpected $o")
      }
    }

    test("Resp parse an array of mixed types") {
      val init = "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n"
      Resp.Array.parse(init.getBytes()) match {
        case ParseComplete(value, _) => 
          value.a match {
            case Some(l) => 
              assertEquals(l, List(Resp.Integer(1L), Resp.Integer(2L), Resp.Integer(3L), Resp.Integer(4L), Resp.BulkString(Some("foobar"))))
            case None => fail("Value Empty")
          }
        case o => fail(s"Unexpected $o")
      }
    }

    test("Resp parse a null array") {
      val init = "*-1\r\n"
      Resp.Array.parse(init.getBytes()) match {
        case ParseComplete(value, _) => 
          assertEquals(value.a, None)
        case o => fail(s"Unexpected $o")
      }
    }

    test("Resp parse an array of arrays") {
      val init = "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n"
      Resp.Array.parse(init.getBytes()).extract match {
        case Some(value) => 
          val expected = Resp.Array(Some(
            List(
              Resp.Array(Some(List(Resp.Integer(1), Resp.Integer(2), Resp.Integer(3)))),
              Resp.Array(Some(List(Resp.SimpleString("Foo"), Resp.Error("Bar"))))
            )
          ))
          assertEquals(value, expected)
        case o => fail(s"Unexpected $o")
      }
    }
}