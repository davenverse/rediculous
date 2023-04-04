package io.chrisdavenport.rediculous

import munit.CatsEffectSuite
import cats.effect._
import fs2.Chunk
import com.comcast.ip4s.{IpAddress, SocketAddress}
import fs2.Pipe
import scala.concurrent.duration._
import java.util.concurrent.TimeoutException

class RedisConnectionSpec extends CatsEffectSuite {
  test("Test Suite"){
    val fakeSocket = new fs2.io.net.Socket[IO]{
      def read(maxBytes: Int): IO[Option[Chunk[Byte]]] = IO(None)
      
      def readN(numBytes: Int): IO[Chunk[Byte]] = ???
      
      def reads: fs2.Stream[IO,Byte] = ???
      
      def endOfInput: IO[Unit] = ???
      
      def endOfOutput: IO[Unit] = ???
      
      def isOpen: IO[Boolean] = ???
      
      def remoteAddress: IO[SocketAddress[IpAddress]] = ???
      
      def localAddress: IO[SocketAddress[IpAddress]] = ???

      def write(bytes: Chunk[Byte]): IO[Unit] = IO.unit
      
      def writes: Pipe[IO,Byte,Nothing] = _.chunks.evalMap(write).drain
      
    }
    val test = RedisConnection.explicitPipelineRequest(fakeSocket, Chunk(Resp.SimpleString("PING")))

    test.map(
      c => assertEquals(c, Chunk())
    )
  }
}