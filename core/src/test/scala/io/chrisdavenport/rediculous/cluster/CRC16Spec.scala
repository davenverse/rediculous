package io.chrisdavenport.rediculous.cluster

import cats.syntax.all._

class CRC16Spec extends munit.FunSuite {
  test("CRC16 outputs the right hash for known input") {
    val out = CRC16.string("123456789")
    val hex = Integer.toHexString(out)
    assert(hex === "31c3")
  }
}