package io.chrisdavenport.rediculous.cluster


class HashSlotSpec extends org.specs2.mutable.Specification {
  "HashSlot.hashKey" should {
    "Find the right key section for a keyslot" in {
      val input = "{user.name}.foo"
      HashSlot.hashKey(input) must_=== "user.name"
    }
    "Find the right key in middle of key" in {
      val input = "bar{foo}baz"
      HashSlot.hashKey(input) must_=== "foo"
    }
    "Find the right key at end of key" in {
      val input = "barbaz{foo}"
      HashSlot.hashKey(input) must_=== "foo"
    }
    "output original key if braces are directly next to each other" in {
      val input = "{}.bar"
      HashSlot.hashKey(input) must_=== input
    }
    "output the full value if no keyslot present" in {
      val input = "bazbarfoo"
      HashSlot.hashKey(input) must_=== input
    }
  }
}