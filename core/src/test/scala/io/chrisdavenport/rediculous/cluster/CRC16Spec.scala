package io.chrisdavenport.rediculous.cluster

class CRC16Spec extends org.specs2.mutable.Specification {
  "CRC16" should {
    "outputs the right hash for known input" in {
      val out = CRC16.string("123456789")
      val hex = Integer.toHexString(out)
      hex must_=== "31c3"
    }
  }
}