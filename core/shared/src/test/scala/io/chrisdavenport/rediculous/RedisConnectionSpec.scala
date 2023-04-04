package io.chrisdavenport.rediculous

import munit.CatsEffectSuite
import cats.effect._
import fs2.{Chunk, Pipe}
import com.comcast.ip4s.{Host, Port,IpAddress, SocketAddress}
import fs2.io.net.{Socket, SocketOption, SocketGroup}

class RedisConnectionSpec extends CatsEffectSuite {

  test("Queued Connection Does Not Hang on EOF"){
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

    val sg = new SocketGroup[IO] {
      def client(to: SocketAddress[Host], options: List[SocketOption]): Resource[IO,Socket[IO]] = Resource.pure(fakeSocket)

      def server(address: Option[Host], port: Option[Port], options: List[SocketOption]): fs2.Stream[IO,Socket[IO]] = ???
      
      def serverResource(address: Option[Host], port: Option[Port], options: List[SocketOption]): Resource[IO,(SocketAddress[IpAddress], fs2.Stream[IO,Socket[IO]])] = ???
      
    }

    RedisConnection.queued[IO].withSocketGroup(sg).build
      .use(c =>
        RedisCommands.ping[RedisIO].run(c)
      ).intercept[RedisError.QueuedExceptionError] // We catch the redis error from the empty returned chunk, previously to #69 this would hang.
  }
}