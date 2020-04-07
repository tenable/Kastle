package com.tenable.library.kafkaclient.client.standard.consumer.units

import com.github.ghik.silencer.silent
import com.tenable.library.kafkaclient.client.standard.consumer.{
  BatchContext,
  GOffsets,
  KafkaUnitTestUtils
}
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

@silent
class FullBatchSpec extends AnyFlatSpec with Matchers {

  "FullBatchSpec.last" should "always return Some" in {
    val res = FullBatch.kafkaProcessable.last(
      new ConsumerRecords[String, String](KafkaUnitTestUtils.buildRecords("t", 1, "some").asJava)
    )

    res should not be empty
  }

  "FullBatchSpec.previous" should "always return None" in {
    val res = FullBatch.kafkaProcessable.previous(
      new ConsumerRecords[String, String](KafkaUnitTestUtils.buildRecords("t", 1, "some").asJava),
      ().asInstanceOf[FullBatch.kafkaProcessable.Ref]
    )

    res shouldBe empty
  }

  "FullBatchSpec.gAtRef" should "always return the full ConsumerRecords with proper offsets" in {
    val records = new ConsumerRecords[String, String](
      (
        KafkaUnitTestUtils.buildRecords("d", 1, ('a' to 'z').toList.map(_.toString): _*) ++
          KafkaUnitTestUtils.buildRecords("a", 3, ('a' to 'd').toList.map(_.toString): _*) ++
          KafkaUnitTestUtils.buildRecords("z", 9, ('a' to 's').toList.map(_.toString): _*)
      ).asJava
    )
    val (gotRecords, offsets) = FullBatch.kafkaProcessable.gAtRef(
      records,
      ().asInstanceOf[FullBatch.kafkaProcessable.Ref]
    )

    offsets shouldBe Map(
      new TopicPartition("d", 1) -> GOffsets(26, 0),
      new TopicPartition("a", 3) -> GOffsets(4, 0),
      new TopicPartition("z", 9) -> GOffsets(19, 0)
    )
    gotRecords shouldBe records
  }

  "FullBatchSpec.previous" should "always return false" in {
    val res = FullBatch.kafkaProcessable.shouldFilter(
      new ConsumerRecords[String, String](KafkaUnitTestUtils.buildRecords("t", 1, "some").asJava),
      BatchContext(Set(new TopicPartition("t", 1)))
    )

    res shouldBe false
  }
}
