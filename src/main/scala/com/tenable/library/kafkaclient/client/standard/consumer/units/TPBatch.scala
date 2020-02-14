package com.tenable.library.kafkaclient.client.standard.consumer.units

import cats.data.NonEmptyList
import com.github.ghik.silencer.silent
import com.tenable.library.kafkaclient.client.standard.consumer._
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable
import scala.collection.JavaConverters._

object TPBatch {
  type TPRecords[K, V] = (TopicPartition, NonEmptyList[ConsumerRecord[K, V]])
  implicit val kafkaProcessable: KafkaProcessable[TPRecords] = new KafkaProcessable[TPRecords] {
    override type Ref = (TopicPartition, mutable.Set[TopicPartition])

    @silent
    def last[K, V](crs: ConsumerRecords[K, V]): Option[Ref] = {
      val partitions = crs.partitions().asScala.filterNot(tp => crs.records(tp).isEmpty)
      partitions.headOption.map((_, partitions.tail))
    }

    def previous[K, V](crs: ConsumerRecords[K, V], ref: Ref): Option[Ref] = {
      ref._2.headOption.map((_, ref._2.tail))
    }

    @silent
    def gAtRef[K, V](
        crs: ConsumerRecords[K, V],
        ref: Ref
    ): ((TopicPartition, NonEmptyList[ConsumerRecord[K, V]]), Map[TopicPartition, GOffsets]) = {
      val tp      = ref._1
      val records = NonEmptyList.fromListUnsafe(crs.records(tp).asScala.toList)
      val offsets = Map(tp -> GOffsets(records.last.offset() + 1, records.head.offset()))

      ((tp, records), offsets)
    }

    override def shouldFilter[K, V](
        g: (TopicPartition, NonEmptyList[ConsumerRecord[K, V]]),
        ctx: BatchContext
    ): Boolean = {
      ctx.skippingPartitions(new TopicPartition(g._1.topic(), g._1.partition()))
    }
  }
}
