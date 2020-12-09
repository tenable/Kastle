package com.tenable.library.kafkaclient.client.standard.consumer.units

import cats.data.NonEmptyList
import com.tenable.library.kafkaclient.client.standard.consumer.{ BatchContext, GOffsets, KafkaUnitTestUtils }
import org.apache.kafka.clients.consumer.{ ConsumerRecord, ConsumerRecords }
import org.apache.kafka.common.TopicPartition

import scala.jdk.CollectionConverters._
import scala.collection.mutable
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TPBatchSpec extends AnyFlatSpec with Matchers {

  "TPBatch.last" should "return None if consumer records are empty" in {
    val crs = new ConsumerRecords[String, String](
      Map.empty[TopicPartition, java.util.List[ConsumerRecord[String, String]]].asJava
    )

    val res = TPBatch.kafkaProcessable.last(crs)
    res shouldBe empty
  }

  it should "return Some with one topic partition batch if consumer records is not empty" in {
    val crs = new ConsumerRecords[String, String](
      (
        KafkaUnitTestUtils.buildRecords("t", 1, "some") ++
          KafkaUnitTestUtils.buildRecords("s", 2, "some")
      ).asJava
    )

    val res = TPBatch.kafkaProcessable.last(crs)
    res should not be empty
  }

  "TPBatch.previous" should "return all topic partitions if called continuously until None is received" in {
    val crs = new ConsumerRecords[String, String](
      (
        KafkaUnitTestUtils.buildRecords("t", 1, "some") ++
          KafkaUnitTestUtils.buildRecords("s", 2, "some") ++
          KafkaUnitTestUtils.buildRecords("r", 3, "some") ++
          KafkaUnitTestUtils.buildRecords("a", 2, "some") ++
          KafkaUnitTestUtils.buildRecords("b", 5, "some")
      ).asJava
    )

    val last         = TPBatch.kafkaProcessable.last(crs)
    val next1        = TPBatch.kafkaProcessable.previous(crs, last.get)
    val next2        = TPBatch.kafkaProcessable.previous(crs, next1.get)
    val next3        = TPBatch.kafkaProcessable.previous(crs, next2.get)
    val next4        = TPBatch.kafkaProcessable.previous(crs, next3.get)
    val noneExpected = TPBatch.kafkaProcessable.previous(crs, next4.get)

    List(last.get, next1.get, next2.get, next3.get, next4.get)
      .map(
        _.asInstanceOf[(TopicPartition, mutable.Set[TopicPartition])]._1
      ) should contain theSameElementsAs crs
      .partitions()
      .asScala

    noneExpected shouldBe empty
  }

  "TPBatch.gAtRef" should "return the entry in the specified ref with the right offsets" in {
    val crs = new ConsumerRecords[String, String](
      (
        KafkaUnitTestUtils.buildRecords("t", 1, "some") ++
          KafkaUnitTestUtils.buildRecords("s", 2, "some") ++
          KafkaUnitTestUtils.buildRecords("r", 3, ('a' to 'z').toList.map(_.toString): _*) ++
          KafkaUnitTestUtils.buildRecords("a", 2, "some") ++
          KafkaUnitTestUtils.buildRecords("b", 5, "some")
      ).asJava
    )

    val expectedTp = new TopicPartition("r", 3)
    val res = TPBatch
      .kafkaProcessable
      .gAtRef(crs, (expectedTp, mutable.Set.empty).asInstanceOf[TPBatch.kafkaProcessable.Ref])
    res._2 shouldBe Map(expectedTp -> GOffsets(26, 0))
    res._1._1 shouldBe expectedTp
    res._1._2.map(_.value()) shouldBe NonEmptyList.of("a", ('b' to 'z').toList.map(_.toString): _*)
  }

  "TPBatch.shouldFilter" should "return true if TP is in the skipping partitions in context" in {
    val expectedTp = new TopicPartition("r", 3)
    val res = TPBatch
      .kafkaProcessable
      .shouldFilter(
        (expectedTp, NonEmptyList.of(new ConsumerRecord("r", 3, 30, "a", "a"))),
        BatchContext(Set(expectedTp))
      )
    res shouldBe true
  }

  it should "return false if tp is not in the skipping partition in context" in {
    val expectedTp = new TopicPartition("r", 3)
    val otherTp    = new TopicPartition("r", 4)
    val res = TPBatch
      .kafkaProcessable
      .shouldFilter(
        (expectedTp, NonEmptyList.of(new ConsumerRecord("r", 3, 30, "a", "a"))),
        BatchContext(Set(otherTp))
      )
    res shouldBe false
  }
}
