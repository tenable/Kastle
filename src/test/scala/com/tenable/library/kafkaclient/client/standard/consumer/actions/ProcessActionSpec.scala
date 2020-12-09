package com.tenable.library.kafkaclient.client.standard.consumer.actions

import java.time.Instant

import cats.effect.IO
import com.tenable.library.kafkaclient.client.standard.consumer.{
  BatchContext,
  GOffsets,
  PausedTemporarily
}
import com.tenable.library.kafkaclient.client.standard.consumer.KafkaUnitTestUtils.KafkaConsumerIORecording
import org.apache.kafka.common.TopicPartition

import scala.concurrent.duration.DurationInt
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ProcessActionSpec extends AnyFlatSpec with Matchers {
  val defaultTopic = "test-topic"

  def offsets(
      partition: Int,
      commitOffset: Long,
      rejectOffset: Long
  ): Map[TopicPartition, GOffsets] = {
    Map(new TopicPartition(defaultTopic, partition) -> GOffsets(commitOffset, rejectOffset))
  }

  private val partition1 = offsets(1, 10, 1)
  private val partition2 = offsets(2, 35, 5)
  private val partition3 = offsets(3, 50, 5)

  private val partitionOffsets = partition1 ++ partition2 ++ partition3

  "ProcessAction.interpret" should "execute single action" in {
    val kafkaConsumerIO = new KafkaConsumerIORecording[IO]()
    val tp              = new TopicPartition(defaultTopic, 42)

    val result = ProcessAction
      .interpret[IO](
        kafkaIO = kafkaConsumerIO,
        action = ProcessAction.single(ProcessAction.Commit(), tp),
        offsets = Map(tp -> GOffsets(10, 0))
      )
      .unsafeRunSync()

    result shouldBe BatchContext.empty
    kafkaConsumerIO.recordedActions should contain theSameElementsInOrderAs Seq(
      kafkaConsumerIO.Commit(Map(tp -> 10))
    )
  }

  it should "execute action on all partitions" in {
    val kafkaConsumerIO = new KafkaConsumerIORecording[IO]()

    val result = ProcessAction
      .interpret[IO](
        kafkaIO = kafkaConsumerIO,
        action = ProcessAction.all(ProcessAction.Commit()),
        offsets = partitionOffsets
      )
      .unsafeRunSync()

    result shouldBe BatchContext.empty
    kafkaConsumerIO.recordedActions should contain theSameElementsInOrderAs Seq(
      kafkaConsumerIO.Commit(partitionOffsets.view.mapValues(_.commit).toMap)
    )
  }

  it should "commit all if action is commitAll" in {
    val kafkaConsumerIO = new KafkaConsumerIORecording[IO]()

    val result = ProcessAction
      .interpret[IO](
        kafkaIO = kafkaConsumerIO,
        action = ProcessAction.commitAll,
        offsets = partitionOffsets
      )
      .unsafeRunSync()

    result shouldBe BatchContext.empty
    kafkaConsumerIO.recordedActions should contain theSameElementsInOrderAs Seq(
      kafkaConsumerIO.Commit(partitionOffsets.view.mapValues(_.commit).toMap)
    )
  }

  it should "commit all except provided set of tps if action is commitAllExcept" in {
    val kafkaConsumerIO      = new KafkaConsumerIORecording[IO]()
    val toSkipTopicPartition = partition1.head._1

    val result = ProcessAction
      .interpret[IO](
        kafkaIO = kafkaConsumerIO,
        action = ProcessAction.commitAllExcept(Set(toSkipTopicPartition)),
        offsets = partitionOffsets
      )
      .unsafeRunSync()

    result shouldBe BatchContext.empty
    kafkaConsumerIO.recordedActions should contain theSameElementsInOrderAs Seq(
      kafkaConsumerIO.Commit((partition2 ++ partition3).view.mapValues(_.commit).toMap)
    )
  }

  it should "reject all if action is rejectAll" in {
    val kafkaConsumerIO = new KafkaConsumerIORecording[IO]()
    val error           = "error"

    val result = ProcessAction
      .interpret[IO](
        kafkaIO = kafkaConsumerIO,
        action = ProcessAction.rejectAll(error),
        offsets = partitionOffsets
      )
      .unsafeRunSync()

    result shouldBe BatchContext(partitionOffsets.keySet)
    kafkaConsumerIO.recordedActions should contain theSameElementsInOrderAs Seq(
      kafkaConsumerIO.SeekWithError(partitionOffsets.view.mapValues(_.reject).toMap, error)
    )
  }

  it should "pause all if action is rejectPauseAll" in {
    val kafkaConsumerIO   = new KafkaConsumerIORecording[IO]()
    val error             = "error"
    val pausedTemporarily = PausedTemporarily(Instant.now, duration = 1.second)

    val result = ProcessAction
      .interpret[IO](
        kafkaIO = kafkaConsumerIO,
        action = ProcessAction.rejectPauseAll(pausedTemporarily, error),
        offsets = partitionOffsets
      )
      .unsafeRunSync()

    result shouldBe BatchContext(partitionOffsets.keySet)
    kafkaConsumerIO.recordedActions should contain theSameElementsInOrderAs Seq(
      kafkaConsumerIO.SeekWithError(partitionOffsets.view.mapValues(_.reject).toMap, error),
      kafkaConsumerIO.PauseTPs(partitionOffsets.view.mapValues(_ => pausedTemporarily).toMap)
    )
  }

  it should "reject as set by action rejectSome" in {
    val kafkaConsumerIO = new KafkaConsumerIORecording[IO]()
    val error           = "error"

    val result = ProcessAction
      .interpret[IO](
        kafkaIO = kafkaConsumerIO,
        action = ProcessAction.rejectSome(Set(partition1.head._1), Some(error)),
        offsets = partitionOffsets
      )
      .unsafeRunSync()

    result shouldBe BatchContext(partition1.keySet)
    kafkaConsumerIO.recordedActions should contain theSameElementsInOrderAs Seq(
      kafkaConsumerIO.SeekWithError(partition1.view.mapValues(_.reject).toMap, error)
    )
  }

  it should "commit as set by action commitSome" in {
    val kafkaConsumerIO = new KafkaConsumerIORecording[IO]()

    val result = ProcessAction
      .interpret[IO](
        kafkaIO = kafkaConsumerIO,
        action = ProcessAction.commitSome(Set(partition1.head._1, partition2.head._1)),
        offsets = partitionOffsets
      )
      .unsafeRunSync()

    result shouldBe BatchContext.empty
    kafkaConsumerIO.recordedActions should contain theSameElementsInOrderAs Seq(
      kafkaConsumerIO.Commit(partition1.view.mapValues(_.commit).toMap),
      kafkaConsumerIO.Commit(partition2.view.mapValues(_.commit).toMap)
    )
  }

  it should "reject some and commit the rest as set by action rejectSomeCommitRest" in {
    val kafkaConsumerIO = new KafkaConsumerIORecording[IO]()
    val error           = "error"
    val reject          = Set(partition3.head._1)

    val result = ProcessAction
      .interpret[IO](
        kafkaIO = kafkaConsumerIO,
        action = ProcessAction.rejectSomeCommitRest(reject, error, maybePauseDetails = None),
        offsets = partitionOffsets
      )
      .unsafeRunSync()

    result shouldBe BatchContext(partition3.keySet)
    kafkaConsumerIO.recordedActions should contain theSameElementsInOrderAs Seq(
      kafkaConsumerIO.SeekWithError(partition3.view.mapValues(_.reject).toMap, error),
      kafkaConsumerIO.Commit((partition1 ++ partition2).view.mapValues(_.commit).toMap)
    )
  }

  it should "reject then pause some, and commit the rest as set by action rejectSomeCommitRest" in {
    val kafkaConsumerIO   = new KafkaConsumerIORecording[IO]()
    val pausedTemporarily = PausedTemporarily(Instant.now, duration = 1.second)
    val error             = "error"
    val reject            = Set(partition3.head._1)

    val result = ProcessAction
      .interpret[IO](
        kafkaIO = kafkaConsumerIO,
        action = ProcessAction
          .rejectSomeCommitRest(reject, error, maybePauseDetails = Some(pausedTemporarily)),
        offsets = partitionOffsets
      )
      .unsafeRunSync()

    result shouldBe BatchContext(partition3.keySet)
    kafkaConsumerIO.recordedActions should contain theSameElementsInOrderAs Seq(
      kafkaConsumerIO.SeekWithError(partition3.view.mapValues(_.reject).toMap, error),
      kafkaConsumerIO.PauseTPs(partition3.view.mapValues(_ => pausedTemporarily).toMap),
      kafkaConsumerIO.Commit((partition1 ++ partition2).view.mapValues(_.commit).toMap)
    )
  }

  it should "reject some and commit some set by action rejectSomeCommitSome" in {
    val kafkaConsumerIO = new KafkaConsumerIORecording[IO]()
    val error           = "error"
    val commit          = Set(partition1.head._1, partition2.head._1)
    val reject          = Set(partition3.head._1)

    val result =
      ProcessAction
        .interpret[IO](
          kafkaIO = kafkaConsumerIO,
          action =
            ProcessAction.rejectSomeCommitSome(commit, reject, error, maybePauseDetails = None),
          offsets = partitionOffsets
        )
        .unsafeRunSync()

    result shouldBe BatchContext(partition3.keySet)
    kafkaConsumerIO.recordedActions should contain theSameElementsInOrderAs Seq(
      kafkaConsumerIO.SeekWithError(partition3.view.mapValues(_.reject).toMap, error),
      kafkaConsumerIO.Commit(partition1.view.mapValues(_.commit).toMap),
      kafkaConsumerIO.Commit(partition2.view.mapValues(_.commit).toMap)
    )
  }

  it should "reject then pause some, and commit some set by action rejectSomeCommitSome" in {
    val kafkaConsumerIO   = new KafkaConsumerIORecording[IO]()
    val pausedTemporarily = PausedTemporarily(Instant.now, duration = 1.second)
    val error             = "error"
    val commit            = Set(partition1.head._1, partition2.head._1)
    val reject            = Set(partition3.head._1)

    val result = ProcessAction
      .interpret[IO](
        kafkaIO = kafkaConsumerIO,
        action = ProcessAction
          .rejectSomeCommitSome(commit, reject, error, maybePauseDetails = Some(pausedTemporarily)),
        offsets = partitionOffsets
      )
      .unsafeRunSync()

    result shouldBe BatchContext(partition3.keySet)
    kafkaConsumerIO.recordedActions should contain theSameElementsInOrderAs Seq(
      kafkaConsumerIO.SeekWithError(partition3.view.mapValues(_.reject).toMap, error),
      kafkaConsumerIO.PauseTPs(partition3.view.mapValues(_ => pausedTemporarily).toMap),
      kafkaConsumerIO.Commit(partition1.view.mapValues(_.commit).toMap),
      kafkaConsumerIO.Commit(partition2.view.mapValues(_.commit).toMap)
    )
  }
}
