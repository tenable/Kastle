package com.tenable.library.kafkaclient.client.standard.consumer.units

import com.tenable.library.kafkaclient.client.standard.consumer._
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable
import scala.jdk.CollectionConverters._

object SingleConsumerRecord {
  val kafkaProcessable: KafkaProcessable[ConsumerRecord] = new KafkaProcessable[ConsumerRecord] {
    override type Ref = ((TopicPartition, Int), mutable.Set[TopicPartition])

    def last[K, V](crs: ConsumerRecords[K, V]): Option[Ref] = {
      val partitions = crs.partitions().asScala

      partitions.find(tp => !crs.records(tp).isEmpty).map { initialTp =>
        ((initialTp, crs.records(initialTp).size() - 1), partitions.filterNot(_ == initialTp))
      }
    }

    override def previous[K, V](crs: ConsumerRecords[K, V], ref: Ref): Option[Ref] = {
      val ((tp, idx), remainingTp) = ref
      if (idx - 1 < 0) {
        remainingTp.find(otherTp => !crs.records(otherTp).isEmpty).map { newTp =>
          (
            (newTp, crs.records(newTp).size() - 1),
            remainingTp.filterNot(_ == newTp)
          ) //Using `--` (that uses hashCode) has some funky results. Avoid!
        }
      } else {
        Some(((tp, idx - 1), remainingTp))
      }
    }

    override def gAtRef[K, V](
        crs: ConsumerRecords[K, V],
        ref: Ref
    ): (ConsumerRecord[K, V], Map[TopicPartition, GOffsets]) = {
      val ((tp, idx), _)   = ref
      val currentTpRecords = crs.records(tp)
      val g                = currentTpRecords.get(idx)
      val offsets          = Map(tp -> GOffsets(g.offset() + 1, g.offset()))
      (g, offsets)
    }

    override def shouldFilter[K, V](g: ConsumerRecord[K, V], ctx: BatchContext): Boolean = {
      ctx.skippingPartitions(new TopicPartition(g.topic(), g.partition()))
    }
  }
}
