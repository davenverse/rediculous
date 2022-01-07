package io.chrisdavenport.rediculous

import fs2.Chunk
import cats.data.NonEmptyList
import cats.Applicative

object RespRaw {

  sealed trait Commands[A]
  

  object Commands {
    final case class SingleCommand[A](key: Option[String], command: NonEmptyList[String]) extends Commands[A]
    final case class CompositeCommands[A](commands: Chunk[SingleCommand[_]]) extends Commands[A]

    implicit val rawRespCommandsCtx: RedisCtx[Commands] = new RedisCtx[Commands] {
      def keyed[A: RedisResult](key: String, command: NonEmptyList[String]): Commands[A] = 
        SingleCommand(Some(key), command)
      
      def unkeyed[A: RedisResult](command: NonEmptyList[String]): Commands[A] = 
        SingleCommand(None, command)
    }
    def combine[C](c1: Commands[_], c2: Commands[_]): Commands[C] = (c1, c2) match {
      case (s1: SingleCommand[_], s2: SingleCommand[_]) => CompositeCommands(Chunk(s1, s2))
      case (s1: SingleCommand[_], s2: CompositeCommands[_]) => CompositeCommands(Chunk(s1) ++ s2.commands)
      case (s1: CompositeCommands[_], s2: SingleCommand[_]) => CompositeCommands(s1.commands ++ Chunk(s2))
      case (s1: CompositeCommands[_], s2: CompositeCommands[_]) => CompositeCommands(s1.commands ++ s2.commands)
    }

    implicit val applicative: Applicative[Commands] = new Applicative[Commands]{
      def ap[A, B](ff: Commands[A => B])(fa: Commands[A]): Commands[B] = combine(ff, fa)
      
      def pure[A](x: A): Commands[A] = CompositeCommands[A](Chunk.empty)
      
    }


  }
  case class RawPipeline[A](key: Option[String], commands: Chunk[NonEmptyList[String]]){

    def pipeline[F[_]](c: RedisConnection[F])(implicit F: cats.effect.Concurrent[F]): F[Chunk[Resp]] = 
      RedisConnection.runRequestInternal(c)(commands, key)
    
  }
  object RawPipeline {

    implicit def ctx[F[_]]: RedisCtx[RawPipeline] = {
      new RedisCtx[RawPipeline] {
        def keyed[A: RedisResult](key: String, command: NonEmptyList[String]): RawPipeline[A] = 
          RawPipeline(Some(key), Chunk.singleton(command))
        
        def unkeyed[A: RedisResult](command: NonEmptyList[String]): RawPipeline[A] = 
          RawPipeline(None, Chunk.singleton(command))
      }
    }

    implicit def app[F[_]]: Applicative[RawPipeline] = {
      new Applicative[RawPipeline]{
        def ap[A, B](ff: RawPipeline[A => B])(fa: RawPipeline[A]): RawPipeline[B] = 
          RawPipeline(ff.key.orElse(fa.key), ff.commands ++ fa.commands)
        
        def pure[A](x: A): RawPipeline[A] = RawPipeline(None, Chunk.empty)
        
      }
    }
    
    
    
  }


}