package com.tenable.library.kafkaclient.client.standard.consumer.units

import com.github.ghik.silencer.silent
import com.tenable.library.kafkaclient.client.standard.consumer.{BatchContext, GOffsets, KafkaUnitTestUtils}
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.collection.mutable
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

@silent
class SingleConsumerRecordSpec extends AnyFlatSpec with Matchers {

  "SingleConsumerRecord.last" should "return None if consumer records is empty" in {
    val crs = new ConsumerRecords[String, String](
      Map.empty[TopicPartition, java.util.List[ConsumerRecord[String, String]]].asJava
    )

    val res = SingleConsumerRecord.kafkaProcessable.last(crs)
    res shouldBe empty
  }

  it should "return Some if consumer records is non empty" in {
    val crs = new ConsumerRecords[String, String](
      (
        KafkaUnitTestUtils.buildRecords("t", 1, "a", "b", "c") ++
          KafkaUnitTestUtils.buildRecords("s", 2, "1", "2", "3")
      ).asJava
    )

    val res = SingleConsumerRecord.kafkaProcessable.last(crs)
    res should not be empty
  }

  "SingleConsumerRecord.previous" should "return the full list of entries if calling previous continuously until None is received" in {
    val crs = new ConsumerRecords[String, String](
      (
        KafkaUnitTestUtils.buildRecords("t", 1, "a", "b", "c") ++
          KafkaUnitTestUtils.buildRecords("s", 2, "1", "2", "3", "4")
      ).asJava
    )

    val last         = SingleConsumerRecord.kafkaProcessable.last(crs)
    val next1        = SingleConsumerRecord.kafkaProcessable.previous(crs, last.get)
    val next2        = SingleConsumerRecord.kafkaProcessable.previous(crs, next1.get)
    val next3        = SingleConsumerRecord.kafkaProcessable.previous(crs, next2.get)
    val next4        = SingleConsumerRecord.kafkaProcessable.previous(crs, next3.get)
    val next5        = SingleConsumerRecord.kafkaProcessable.previous(crs, next4.get)
    val next6        = SingleConsumerRecord.kafkaProcessable.previous(crs, next5.get)
    val noneExpected = SingleConsumerRecord.kafkaProcessable.previous(crs, next6.get)

    val tp1 = new TopicPartition("t", 1)
    val tp2 = new TopicPartition("s", 2)

    List(
      List(last.get, next1.get, next2.get, next3.get, next4.get, next5.get, next6.get) //Have to wrap the List in a List to use the oneOf
        .map(_.asInstanceOf[((TopicPartition, Int), mutable.Set[TopicPartition])]._1)
    ) should contain oneOf (
      List((tp1, 2), (tp1, 1), (tp1, 0), (tp2, 3), (tp2, 2), (tp2, 1), (tp2, 0)),
      List((tp2, 3), (tp2, 2), (tp2, 1), (tp2, 0), (tp1, 2), (tp1, 1), (tp1, 0))
    )

    noneExpected shouldBe empty
  }

  "SingleConsumerRecord.gAtRef" should "return the element as specified by the Ref" in {
    val crs = new ConsumerRecords[String, String](
      (
        KafkaUnitTestUtils.buildRecords("t", 1, "a", "b", "c") ++
          KafkaUnitTestUtils.buildRecords("s", 2, "1", "2", "3", "4")
      ).asJava
    )

    val tp1 = new TopicPartition("t", 1)
    val tp2 = new TopicPartition("s", 2)

    (0 to 2).toList.map { idx =>
      val el = SingleConsumerRecord.kafkaProcessable.gAtRef(
        crs,
        ((tp1, idx), mutable.Set.empty).asInstanceOf[SingleConsumerRecord.kafkaProcessable.Ref]
      )
      (el._1.value(), el._2)
    } shouldBe List("a", "b", "c").zipWithIndex.map {
      case (el, idx) => (el, Map(tp1 -> GOffsets(idx + 1L, idx.toLong)))
    }

    (0 to 3).toList.map { idx =>
      val el = SingleConsumerRecord.kafkaProcessable.gAtRef(
        crs,
        ((tp2, idx), mutable.Set.empty).asInstanceOf[SingleConsumerRecord.kafkaProcessable.Ref]
      )
      (el._1.value(), el._2)
    } shouldBe List("1", "2", "3", "4").zipWithIndex.map {
      case (el, idx) => (el, Map(tp2 -> GOffsets(idx + 1L, idx.toLong)))
    }

  }

  "SingleConsumerRecord.shouldFilter" should "return true if TP is in the skipping partitions in context" in {
    val expectedTp = new TopicPartition("r", 3)
    val res = SingleConsumerRecord.kafkaProcessable
      .shouldFilter(new ConsumerRecord("r", 3, 30, "a", "a"), BatchContext(Set(expectedTp)))
    res shouldBe true
  }

  it should "return false if tp is not in the skipping partition in context" in {
    val otherTp    = new TopicPartition("r", 4)
    val res = SingleConsumerRecord.kafkaProcessable
      .shouldFilter(new ConsumerRecord("r", 3, 30, "a", "a"), BatchContext(Set(otherTp)))
    res shouldBe false
  }
}
