package io.chrisdavenport.rediculous.util

import cats.syntax.all._
import fs2._
import fs2.io.net.Socket
import cats.effect._
import com.comcast.ip4s.{IpAddress, SocketAddress}

private[rediculous] trait BufferedSocket[F[_]] extends Socket[F]{
  def buffer(bytes: Chunk[Byte]): F[Unit]
}

private[rediculous] object BufferedSocket{

  def fromSocket[F[_]: Concurrent](s: Socket[F]): F[BufferedSocket[F]] = 
    Ref[F].of(Option.empty[Chunk[Byte]]).map{r => new Impl(s, r)}


  private class Impl[F[_]: Concurrent](socket: Socket[F], buffer: Ref[F, Option[Chunk[Byte]]]) extends BufferedSocket[F]{
    def buffer(bytes: Chunk[Byte]): F[Unit] = buffer.update{
      case Some(b1) => (b1 ++ bytes).some
      case None => bytes.some
    }

    def takeBuffer: F[Option[Chunk[Byte]]] = buffer.modify{
      x => (None, x)
    }

    // This can return more bytes than max bytes, may want to refine this later
    def read(maxBytes: Int): F[Option[Chunk[Byte]]] = takeBuffer.flatMap{
      case Some(value) => value.some.pure[F]
      case None => socket.read(maxBytes)
    }
    
    def readN(numBytes: Int): F[Chunk[Byte]] = takeBuffer.flatMap{
      case Some(value) => value.pure[F]
      case None => socket.readN(numBytes)
    }
    
    def reads: Stream[F,Byte] = Stream.repeatEval(read(8192)).unNoneTerminate.unchunks
    
    def endOfInput: F[Unit] = socket.endOfInput
    
    def endOfOutput: F[Unit] = socket.endOfOutput
    
    def isOpen: F[Boolean] = socket.isOpen
    
    def remoteAddress: F[SocketAddress[IpAddress]] = socket.remoteAddress
    
    def localAddress: F[SocketAddress[IpAddress]] = socket.localAddress
    
    def write(bytes: Chunk[Byte]): F[Unit] = socket.write(bytes)
    
    def writes: Pipe[F,Byte,Nothing] = socket.writes
    
  }

}