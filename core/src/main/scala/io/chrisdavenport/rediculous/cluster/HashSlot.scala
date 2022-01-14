package io.chrisdavenport.rediculous.cluster

import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import scodec.bits.ByteVector

/**
  * HashSlots are values 0-16384, They are the result of parsing keys, and then
  * via Cluster HashSlotMap we know to which server key operations should be sent to.
  * 
  * Multi-Key operations are expected to operate on only the same HashSlot, but may work
  * if all keys are present on the same node, users should take care.
  * 
  * HashSlots are defined as Either the entire key, or the first subsection of the string
  * contained in curly brackets {user.name}.id will be keyed via {user.name}, if the first
  * curly braces are empty, then the original string is used.
  * 
  * Afterwards the has approach is to Apply CRC16 and then modulo the table of HashSlots 
  * to find the HashSlot associated with a particular key.
  * 
  * Then Clients Communicate by Looking at their HashSlotMap and Sending Operations to the
  * appropriate nodes in the cluster.
  */
object HashSlot {
  
  def find(key: ByteVector)(implicit C: Charset = StandardCharsets.UTF_8): Int = {
    val toHash = hashKey(key)
    CRC16.bytevector(toHash) % 16384
  }
  

  
  def hashKey(key: ByteVector): ByteVector = {
    val s = key.indexOfSlice(ByteVector('{'))
    if (s >= 0) {
      val e = key.indexOfSlice(ByteVector('}'))
      if (e >= 0 && e != s + 1) key.slice(s + 1, e)
      else key
    } else key
  }
}