package com.tenable.library.kafkaclient.client.standard.consumer

import java.util.{ List => JList }

import cats.data.StateT
import cats.effect.IO
import com.tenable.library.kafkaclient.client.standard.consumer.KafkaUnitTestUtils.buildRecords
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition

import scala.jdk.CollectionConverters._
import scala.collection.mutable.ArrayBuffer
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class KafkaProcessableSpec extends AnyFlatSpec with Matchers {
  case class TestRecord[K, V](consumerRecord: ConsumerRecord[K, V])
  case class TestRecordRef(topicPartitionIndex: Int, index: Int)

  private implicit val testProcessable: KafkaProcessable[TestRecord] =
    new KafkaProcessable[TestRecord] {
      override type Ref = TestRecordRef
      private def sort[K, V](
        consumerRecords: ConsumerRecords[K, V]
      ): IndexedSeq[(TopicPartition, IndexedSeq[ConsumerRecord[K, V]])] = {
        val sortedTps =
          consumerRecords.partitions().asScala.toIndexedSeq.sortBy(x => (x.topic(), x.partition()))
        sortedTps.map { tp =>
          val records = consumerRecords.records(tp).asScala.toIndexedSeq
          (tp, records)
        }
      }

      def last[K, V](consumerRecords: ConsumerRecords[K, V]): Option[Ref] = {
        val sorted = sort(consumerRecords)
        val size   = sorted.size
        if (consumerRecords.isEmpty) { None }
        else { Some(TestRecordRef(size - 1, consumerRecords.records(sorted(size - 1)._1).size() - 1)) }
      }

      def previous[K, V](consumerRecords: ConsumerRecords[K, V], ref: Ref): Option[Ref] = {
        val sorted = sort(consumerRecords)

        if (ref.index - 1 >= 0) { Some(TestRecordRef(ref.topicPartitionIndex, ref.index - 1)) }
        else {
          if (ref.topicPartitionIndex - 1 < 0) { None }
          else {
            Some(
              TestRecordRef(
                ref.topicPartitionIndex - 1,
                sorted(ref.topicPartitionIndex - 1)._2.size - 1
              )
            )
          }
        }
      }

      def gAtRef[K, V](
        consumerRecords: ConsumerRecords[K, V],
        ref: TestRecordRef
      ): (TestRecord[K, V], Map[TopicPartition, GOffsets]) = {
        val sorted = sort(consumerRecords)
        val record = sorted(ref.topicPartitionIndex)._2(ref.index)
        (
          TestRecord(record),
          Map(
            new TopicPartition(record.topic(), record.partition()) -> GOffsets(
              record.offset() + 1,
              record.offset()
            )
          )
        )
      }

      def shouldFilter[K, V](g: TestRecord[K, V], ctx: BatchContext): Boolean =
        ctx.skippingPartitions(
          new TopicPartition(g.consumerRecord.topic(), g.consumerRecord.partition())
        )
    }

  "KafkaProcessable.build" should "return an empty set if empty input" in {
    val input =
      new ConsumerRecords(Map.empty[TopicPartition, JList[ConsumerRecord[String, String]]].asJava)
    val res = KafkaProcessable.build[String, String, TestRecord](input)

    res shouldBe None
  }

  it should "be able to build and run the stream in the right order" in {
    val tB2 = buildRecords("tB", 2, ('A' to 'Z').map(_.toString): _*)
    val tB1 = buildRecords("tB", 1, ('a' to 'z').map(_.toString): _*)
    val tA1 = buildRecords("tA", 1, ('1' to '9').map(_.toString): _*)

    val input = new ConsumerRecords((tB1 ++ tB2 ++ tA1).asJava)

    //Build cofree
    val (rejectOffsets, batch) = KafkaProcessable.build[String, String, TestRecord](input).get

    //Run cofree with stateT to gather results
    val res = KafkaProcessable
      .run[StateT[IO, ArrayBuffer[String], *], String, String, TestRecord](
        batch,
        { case (g, _) =>
          StateT.apply[IO, ArrayBuffer[String], BatchContext](s =>
            IO.pure((s.+=(g.consumerRecord.value()), BatchContext.empty))
          )
        }
      )
      .run(ArrayBuffer.empty)
      .unsafeRunSync()
      ._1

    val expected = (tA1 ++ tB1 ++ tB2).values.flatMap(_.asScala).toList

    res should contain theSameElementsInOrderAs expected.map(_.value())
    rejectOffsets shouldBe (tA1 ++ tB1 ++ tB2).keys.map(_ -> 0).toMap //0 being head
  }

  it should "be able to build and run the stream in the right order, filtering accordingly" in {
    val tB2 = buildRecords("tB", 2, ('A' to 'Z').map(_.toString): _*)
    val tB1 = buildRecords("tB", 1, ('a' to 'z').map(_.toString): _*)
    val tA1 = buildRecords("tA", 1, ('1' to '9').map(_.toString): _*)

    val input = new ConsumerRecords((tB1 ++ tB2 ++ tA1).asJava)

    //Build cofree
    val (rejectOffsets, batch) = KafkaProcessable.build[String, String, TestRecord](input).get

    //Run cofree with stateT to gather results
    val res = KafkaProcessable
      .run[StateT[IO, ArrayBuffer[String], *], String, String, TestRecord](
        batch,
        { case (g, _) =>
          StateT.apply[IO, ArrayBuffer[String], BatchContext] { s =>
            if (g.consumerRecord.topic() == "tA" && g.consumerRecord.offset() == 0) {
              IO.pure((s, BatchContext(Set(new TopicPartition("tA", 1)))))
            } else { IO.pure((s.+=(g.consumerRecord.value()), BatchContext.empty)) }
          }
        }
      )
      .run(ArrayBuffer.empty)
      .unsafeRunSync()
      ._1

    val expected = (tB1 ++ tB2).values.flatMap(_.asScala).toList

    res should contain theSameElementsInOrderAs expected.map(_.value())
    rejectOffsets shouldBe (tA1 ++ tB1 ++ tB2).keys.map(_ -> 0).toMap //0 being head
  }
}
