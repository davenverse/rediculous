package io.chrisdavenport.rediculous

import cats._
import cats.implicits._
import cats.data.{NonEmptyList, Chain, Nested, Kleisli}
import cats.effect._

case class RedisPipeline2[F[_], A](value: F[Ref[F, (Chain[NonEmptyList[String]], Option[String])] => F[RedisTransaction.Queued[A]]])

object RedisPipeline2 {

  implicit def ctx[F[_]: Monad]: RedisCtx[RedisPipeline2[F, *]] =  new RedisCtx[RedisPipeline2[F, *]]{
    def keyed[A: RedisResult](key: String, command: NonEmptyList[String]): RedisPipeline2[F, A] = RedisPipeline2[F, A](Applicative[F].pure{
      (ref: Ref[F, (Chain[NonEmptyList[String]], Option[String])]) => 
        ref.modify{
          case (base, value) => 
            val newCommands = base.append(command)
            (newCommands, value.orElse(Some(key))) -> newCommands.size
        }.map(i => 
          RedisTransaction.Queued(l => RedisResult[A].decode(l(i.toInt)))
        )
    })

    def unkeyed[A: RedisResult](command: NonEmptyList[String]): RedisPipeline2[F, A] =  RedisPipeline2[F, A](Applicative[F].pure{
      (ref: Ref[F, (Chain[NonEmptyList[String]], Option[String])]) => 
        ref.modify{
          case (base, value) => 
            val newCommands = base.append(command)
            (newCommands, value) -> newCommands.size
        }.map(i => 
          RedisTransaction.Queued(l => RedisResult[A].decode(l(i.toInt)))
        )
    })
  }

  implicit def applicative[F[_]: Applicative]: Applicative[RedisPipeline2[F, *]] = new Applicative[RedisPipeline2[F, *]]{
    def pure[A](a: A) = RedisPipeline2(Applicative[F].pure(_ => Applicative[F].pure(Monad[RedisTransaction.Queued].pure(a))))

    override def ap[A, B](ff: RedisPipeline2[F, A => B])(fa: RedisPipeline2[F, A]): RedisPipeline2[F, B] =
      RedisPipeline2(
        (
          ff.value,
          fa.value
        ).mapN{
          case (qFF, qFA) => 
            {case (ref) =>
              (qFF(ref), qFA(ref)).mapN{
                case (qff, qfa) => qff.ap(qfa)
              }
            }
        }
      )
  }

  def toRedis[F[_]: Concurrent, A](pipeline: RedisPipeline2[F, A]): Redis[F, A] = Redis(Kleisli{connection => 
    Concurrent[F].ref((Chain.empty[NonEmptyList[String]], Option.empty[String])).flatMap(
      ref => 
        pipeline.value.flatMap{f => 
          f(ref).flatMap{ queued => 
            ref.get.flatMap{
              case ((chain, key)) => 
                val commands = chain.toList.toNel
                commands.traverse(nelCommands => 
                  RedisConnection.runRequestInternal(connection)(nelCommands, key) // We Have to Actually Send A Command
                  .flatMap{nel => RedisConnection.closeReturn[F, A](queued.f(nel.toList))}
                ).flatMap{
                  case Some(a) => a.pure[F]
                  case None => Concurrent[F].raiseError(RedisError.Generic("Rediculous: Attempted to Pipeline Empty Command"))
                }
            }
          }
        }
    )
  })

}