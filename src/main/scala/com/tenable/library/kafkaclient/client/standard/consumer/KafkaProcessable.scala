package com.tenable.library.kafkaclient.client.standard.consumer

import cats.effect.Sync
import cats.free.Cofree
import cats.instances.option._
import cats.syntax.functor._
import cats.syntax.monoid._
import cats.{Eval, Monad, Now, ~>}
import com.tenable.library.kafkaclient.client.standard.consumer.units.TPBatch.TPRecords
import com.tenable.library.kafkaclient.client.standard.consumer.units.{
  FullBatch,
  SingleConsumerRecord,
  TPBatch
}
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}
import org.apache.kafka.common.TopicPartition

import scala.jdk.CollectionConverters._

trait KafkaProcessable[G[_, _]] {
  type Ref

  def last[K, V](consumerRecords: ConsumerRecords[K, V]): Option[Ref]

  def previous[K, V](consumerRecords: ConsumerRecords[K, V], ref: Ref): Option[Ref]

  def gAtRef[K, V](
      consumerRecords: ConsumerRecords[K, V],
      ref: Ref
  ): (G[K, V], Map[TopicPartition, GOffsets])

  def shouldFilter[K, V](g: G[K, V], ctx: BatchContext): Boolean
}

object KafkaProcessable {
  def apply[G[_, _]](implicit KP: KafkaProcessable[G]): KafkaProcessable[G] = KP

  implicit val kafkaProcessableSingle: KafkaProcessable[ConsumerRecord] =
    SingleConsumerRecord.kafkaProcessable
  implicit val kafkaProcessableTPRecords: KafkaProcessable[TPRecords] = TPBatch.kafkaProcessable
  implicit val kafkaProcessableFullBatch: KafkaProcessable[ConsumerRecords] =
    FullBatch.kafkaProcessable

  type GAndOffsets[G] = (G, Map[TopicPartition, GOffsets])
  type Batch[G]       = Cofree[Option, GAndOffsets[G]]
  type RejectOffsets  = Map[TopicPartition, Long]

  def build[K, V, G[_, _]: KafkaProcessable](
      crs: ConsumerRecords[K, V]
  ): Option[(RejectOffsets, Batch[G[K, V]])] = {
    val KP = KafkaProcessable[G]

    val rejectOffsets: RejectOffsets =
      crs.partitions.asScala
        .flatMap(p => crs.records(p).asScala.headOption.map(record => p -> record.offset))
        .toMap

    KP.last(crs)
      .map { start =>
        Cofree.ana[Option, KP.Ref, GAndOffsets[G[K, V]]](start)(
          { ref =>
            KP.previous(crs, ref)
          },
          { ref =>
            KP.gAtRef(crs, ref)
          }
        )
      }
      .map((rejectOffsets, _))
  }

  def run[M[_]: Sync, K, V, G[_, _]](
      cofree: Batch[G[K, V]],
      runG: GAndOffsets[G[K, V]] => M[BatchContext]
  )(implicit KP: KafkaProcessable[G]): M[Unit] = {
    val M = Sync[M]

    Cofree
      .cataM[Option, M, GAndOffsets[G[K, V]], BatchContext](cofree) { case ((g, offsets), acc) =>
        acc match {
          case Some(ctx) if KP.shouldFilter(g, ctx) => Monad[M].pure(ctx)
          case other =>
            val currentCtx = other.getOrElse(BatchContext.empty)
            runG { (g, offsets) }.map(_ |+| currentCtx)
        }
      }(new (Eval ~> M) {
        override def apply[A](fa: Eval[A]): M[A] = fa match {
          case Now(value) => M.pure(value)
          case other      => M.delay(other.value)
        }
      })
      .void
  }
}
