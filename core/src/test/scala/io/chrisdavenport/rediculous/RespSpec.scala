package io.chrisdavenport.rediculous

import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import io.chrisdavenport.rediculous.Resp.ParseComplete
import io.chrisdavenport.rediculous.Resp.ParseIncomplete
import io.chrisdavenport.rediculous.Resp.ParseError

class RespSpec extends Specification with ScalaCheck {
  "Resp" should {
    "parse a simple-string" in {
      Resp.SimpleString.parse("+OK\r\n".getBytes()) match {
        case ParseComplete(value, _) => 
          value.value must_=== "OK"
        case _ => ko
      }
    }
    "round-trip a simple-string" >> prop { s: String => (!s.contains("\r") && !s.contains("\n")) ==> {
      // val initS = s.replace("\r", "").replace("\n", "") // simple strings cannot contain new lines
      val init = Resp.SimpleString(s)
      Resp.SimpleString.parse(
        Resp.SimpleString.encode(init)
      ) match {
        case ParseComplete(value, rest) => 
          (value must_=== init).and(
            rest must beEmpty
          )
        case o => ko(s"Unexpected: $o")
      }
    }}

    "parse an error" in {
      val s = "-Error message\r\n"
      Resp.Error.parse(s.getBytes()) match {
        case ParseComplete(value, _) => 
          value.value must_=== "Error message"
        case o => ko(s"Unexpected: $o")
      }
    }

    "round-trip an error" in prop{s : String =>  (!s.contains("\r") && !s.contains("\n")) ==> {
      val init = Resp.Error(s)
      Resp.Error.parse(
        Resp.Error.encode(init)
      ) match {
        case ParseComplete(value, rest) => 
          (value.value must_=== init.value).and(
            rest must beEmpty
          )
        case o => ko(s"Unexpected $o")
      }
    }}

    "parse an integer" in {
      val s = ":1000\r\n"
      Resp.Integer.parse(s.getBytes()) match {
        case ParseComplete(value, _) => 
          value.long must_=== 1000L
        case o => ko(s"Unexpected $o")
      }
    }

    "encode an integer" in {
      val x = 8971392965300387418L
      val arr = Resp.Integer.encode(Resp.Integer(x))
      val s = new String(arr)
      s must_=== ":8971392965300387418\r\n"
    }

    "round-trip an integer" >> prop {l: Long => 
      val init = Resp.Integer(l)
      Resp.Integer.parse(
        Resp.Integer.encode(init)
      ) match {
        case ParseComplete(value, rest) => 
          (value must_=== init).and(
            rest must beEmpty
          )
        case o => ko(s"Got Unexpected Result $o")
      }
    }

    "parse a bulk string" in {
      val s = "$6\r\nfoobar\r\n"
      Resp.BulkString.parse(s.getBytes()) match {
        case ParseComplete(value, _) => 
        value.value must beSome.like{ case s2 => s2 must_=== "foobar"}
        case o => ko(s"Unexpected $o")
      }
    }

    "parse an empty bulk string" in {
      val s = "$0\r\n\r\n"
      Resp.BulkString.parse(s.getBytes()) match {
        case ParseComplete(value, _) => 
        value.value must beSome.like{ case s2 => s2 must_=== ""}
        case o => ko(s"Unexpected $o")
      }
    }

    "parse a null bulk string" in {
      val s = "$-1\r\n"
      Resp.BulkString.parse(s.getBytes()) match {
        case ParseComplete(value, _) => 
        value.value must beNone
        case o => ko(s"Unexpected $o")
      }

    }

    "round-trip a bulk-string" >> prop{ s : String => 
      val init = Resp.BulkString(Some(s))
      Resp.BulkString.parse(
        Resp.BulkString.encode(init)
      ) match {
        case ParseComplete(value, rest) => 
          (value must_=== init).and(
            rest must beEmpty
          )
        case p@ParseIncomplete(_) => ko(s"Got Incomplete Result $p")
        case e@ParseError(_,_) => ko(s"Got ParseError $e")
      }
    }

    "parse an empty array" in {
      val init = "*0\r\n"
      Resp.Array.parse(init.getBytes()) match {
        case ParseComplete(value, _) => 
          value.a must beSome.like{ case l => l must beEmpty}
        case o => ko(s"Unexpected $o")
      }
    }

    "parse an array of bulk strings" in {
      val init = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
      Resp.Array.parse(init.getBytes()) match {
        case ParseComplete(value, _) => 
          value.a must beSome.like{ case l => l must_=== List(Resp.BulkString(Some("foo")), Resp.BulkString(Some("bar")))}
        case o => ko(s"Unexpected $o")
      }
    }

    "parse an array of integers" in {
      val init = "*3\r\n:1\r\n:2\r\n:3\r\n"
      Resp.Array.parse(init.getBytes()) match {
        case ParseComplete(value, _) => 
          value.a must beSome.like{ case l => 
            l must_=== List(Resp.Integer(1L), Resp.Integer(2L), Resp.Integer(3L))}
        case o => ko(s"Unexpected $o")
      }
    }

    "parse an array of mixed types" in {
      val init = "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n"
      Resp.Array.parse(init.getBytes()) match {
        case ParseComplete(value, _) => 
          value.a must beSome.like{ case l => 
            l must_=== List(Resp.Integer(1L), Resp.Integer(2L), Resp.Integer(3L), Resp.Integer(4L), Resp.BulkString(Some("foobar")))}
        case o => ko(s"Unexpected $o")
      }
    }

    "parse a null array" in {
      val init = "*-1\r\n"
      Resp.Array.parse(init.getBytes()) match {
        case ParseComplete(value, _) => 
          value.a must beNone
        case o => ko(s"Unexpected $o")
      }
    }

    "parse an array of arrays" in {
      val init = "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n"
      Resp.Array.parse(init.getBytes()) match {
        case ParseComplete(value, _) => 
          value must_=== Resp.Array(Some(
            List(
              Resp.Array(Some(List(Resp.Integer(1), Resp.Integer(2), Resp.Integer(3)))),
              Resp.Array(Some(List(Resp.SimpleString("Foo"), Resp.Error("Bar"))))
            )
          ))
        case o => ko(s"Unexpected $o")
      }
    }
  }
}