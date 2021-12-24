package io.chrisdavenport.rediculous.util

import cats._
import cats.syntax.all._
import fs2._
import fs2.io.net.Socket
import cats.effect._
import cats.effect.std.Queue
import com.comcast.ip4s.{IpAddress, SocketAddress}

private[rediculous] trait BufferedSocket[F[_]] extends Socket[F]{
  def buffer(bytes: Chunk[Byte]): F[Unit]
}

private[rediculous] object BufferedSocket{

  def fromSocket[F[_]: Concurrent](s: Socket[F]): F[BufferedSocket[F]] = 
    Queue.unbounded[F, Chunk[Byte]].map{q => new Impl(s, q)}


  private class Impl[F[_]: Concurrent](socket: Socket[F], buffer: Queue[F, Chunk[Byte]]) extends BufferedSocket[F]{
    def buffer(bytes: Chunk[Byte]): F[Unit] = buffer.offer(bytes)

    // This can return more bytes than max bytes, may want to refine this later
    def read(maxBytes: Int): F[Option[Chunk[Byte]]] = buffer.tryTake.flatMap{
      case s@Some(value) => value.some.pure[F]
      case None => socket.read(maxBytes)
    }
    
    def readN(numBytes: Int): F[Chunk[Byte]] = buffer.tryTake.flatMap{
      case s@Some(value) => value.pure[F]
      case None => socket.readN(numBytes)
    }
    
    def reads: Stream[F,Byte] = Stream.repeatEval(read(8192)).unNoneTerminate.unchunks
    
    def endOfInput: F[Unit] = socket.endOfInput
    
    def endOfOutput: F[Unit] = socket.endOfOutput
    
    def isOpen: F[Boolean] = socket.isOpen
    
    def remoteAddress: F[SocketAddress[IpAddress]] = socket.remoteAddress
    
    def localAddress: F[SocketAddress[IpAddress]] = socket.localAddress
    
    def write(bytes: Chunk[Byte]): F[Unit] = socket.write(bytes)
    
    def writes: Pipe[F,Byte,INothing] = socket.writes
    
  }

}