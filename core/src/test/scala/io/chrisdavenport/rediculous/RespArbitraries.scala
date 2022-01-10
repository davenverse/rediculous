package io.chrisdavenport.rediculous

import cats.syntax.all._
import org.scalacheck._
import org.scalacheck.{Gen => G}
import scodec.bits._
import org.scalacheck.cats.implicits._

object RespArbitraries {

  implicit val arbitraryByteVector = Arbitrary(
    G.containerOf[Array, Byte](Arbitrary.arbByte.arbitrary).map(
      array => ByteVector.view(array)
    )
  )


  implicit val arbitrarySimpleString: Arbitrary[Resp.SimpleString] = Arbitrary(
    Gen.asciiStr.map(_.replace("\r", "").replace("\n", "")).map(Resp.SimpleString(_))
  )

  implicit val arbitraryError: Arbitrary[Resp.Error] = Arbitrary(
    Gen.asciiStr.map(_.replace("\r", "").replace("\n", "")).map(Resp.Error(_))
  )

  implicit val aribitraryBulkString: Arbitrary[Resp.BulkString] = Arbitrary(
    Gen.frequency(
      80 -> arbitraryByteVector.arbitrary.map(bv => Resp.BulkString(Some(bv))),
      20 -> Gen.const(Resp.BulkString(None))
    )
  )

  implicit val arbInteger: Arbitrary[Resp.Integer] = Arbitrary(
    Gen.posNum[Long].map(l => Resp.Integer(l))
  )

  val nonList = Gen.oneOf(
    arbitrarySimpleString.arbitrary.widen[Resp],
    arbitraryError.arbitrary.widen[Resp],
    aribitraryBulkString.arbitrary.widen[Resp],
    arbInteger.arbitrary.widen[Resp],
  )

  val listGen = Gen.recursive[Resp.Array](arrayGen => 
    Gen.option(Gen.containerOf[List, Resp](nonList)).map(v => Resp.Array(v))
  )

  // TODO figure out how to do this without stack overflow
  implicit lazy val  basicArray: Arbitrary[Resp.Array] = Arbitrary(
    listGen
  )

  implicit lazy val resp: Arbitrary[Resp] = Arbitrary(
    Gen.lzy(
      Gen.oneOf(
        arbitrarySimpleString.arbitrary.widen[Resp],
        arbitraryError.arbitrary.widen[Resp],
        aribitraryBulkString.arbitrary.widen[Resp],
        arbInteger.arbitrary.widen[Resp],
        listGen.widen[Resp]
      )
    )
  )






}