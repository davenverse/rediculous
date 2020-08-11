package io.chrisdavenport.rediculous.cluster

object HashSlot {
  
  def apply(key: String): Int = {
    val toHash = hashKey(key)
    CRC16(toHash)
  }
  
  def hashKey(key: String): String = {
    val s = key.indexOf('{')
    if (s >= 0) {
      val e = key.indexOf('}')
      if (e >= 0 && e != s + 1) key.substring(s, e)
      else key
    } else key
  }
}