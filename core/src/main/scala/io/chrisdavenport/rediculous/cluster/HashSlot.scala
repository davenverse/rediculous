package io.chrisdavenport.rediculous.cluster

import java.nio.charset.Charset
import java.nio.charset.StandardCharsets

object HashSlot {
  
  def find(key: String)(implicit C: Charset = StandardCharsets.UTF_8): Int = {
    val toHash = hashKey(key)
    CRC16.string(toHash) % 16384
  }
  
  def hashKey(key: String): String = {
    val s = key.indexOf('{')
    if (s >= 0) {
      val e = key.indexOf('}')
      if (e >= 0 && e != s + 1) key.substring(s + 1, e)
      else key
    } else key
  }
}