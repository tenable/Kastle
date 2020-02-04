package com.tenable.library.kafkaclient.client.standard.consumer

import cats.kernel.Monoid
import org.apache.kafka.common.TopicPartition

case class GOffsets(commit: Long, reject: Long)

case class BatchContext(skippingPartitions: Set[TopicPartition])
object BatchContext {
  val empty = BatchContext(Set.empty)

  implicit val monoidBC: Monoid[BatchContext] = new Monoid[BatchContext] {
    def empty: BatchContext = BatchContext(Set.empty)
    def combine(left: BatchContext, right: BatchContext): BatchContext = {
      if (right.skippingPartitions.isEmpty) {
        left
      } else {
        left.copy(skippingPartitions = left.skippingPartitions ++ right.skippingPartitions)
      }
    }
  }
}
