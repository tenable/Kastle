package com.tenable.library.kafkaclient.client.standard.consumer.units

import com.tenable.library.kafkaclient.client.standard.consumer._
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.TopicPartition

import scala.jdk.CollectionConverters._

object FullBatch {
  val kafkaProcessable: KafkaProcessable[ConsumerRecords] = new KafkaProcessable[ConsumerRecords] {
    override type Ref = Unit

    def last[K, V](consumerRecords: ConsumerRecords[K, V]): Option[Ref]               = Some(())
    def previous[K, V](consumerRecords: ConsumerRecords[K, V], ref: Ref): Option[Ref] = None

    def gAtRef[K, V](
        consumerRecords: ConsumerRecords[K, V],
        ref: Ref
    ): (ConsumerRecords[K, V], Map[TopicPartition, GOffsets]) = {
      val offsets = consumerRecords
        .partitions()
        .asScala
        .flatMap { tp =>
          val records = consumerRecords.records(tp)
          if (records.isEmpty) {
            None
          } else {
            Some(
              tp -> GOffsets(records.get(records.size() - 1).offset() + 1, records.get(0).offset())
            )
          }
        }
        .toMap
      (consumerRecords, offsets)
    }
    override def shouldFilter[K, V](g: ConsumerRecords[K, V], ctx: BatchContext): Boolean = false
  }
}
