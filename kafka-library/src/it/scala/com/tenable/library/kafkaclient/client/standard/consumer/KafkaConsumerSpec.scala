package com.tenable.library.kafkaclient.client.standard.consumer

import java.time.Instant
import java.util
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue

import cats.syntax.apply._
import com.tenable.library.kafkaclient.client.standard.consumer.actions.ProcessAction
import com.tenable.library.kafkaclient.config.{KafkaConsumerConfig}
import com.tenable.library.kafkaclient.testhelpers.AsyncIntegrationSpec
import com.tenable.library.kafkaclient.testhelpers.GeneralKafkaHelpers._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.{Cluster, TopicPartition}
import org.scalatest.compatible.Assertion
import KafkaProcessable._
import cats.effect.IO
import cats.syntax.applicativeError._
import cats.instances.list._
import cats.syntax.traverse._
import com.tenable.library.kafkaclient.client.standard.KafkaConsumerIO
import com.tenable.library.kafkaclient.client.standard.consumer.units.TPBatch.TPRecords

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.global
import com.tenable.library.kafkaclient.config.TopicDefinitionDetails
import net.manub.embeddedkafka.EmbeddedKafka
import org.scalatest.BeforeAndAfterAll

class TestPartitioner2Partitions extends Partitioner {
  override def partition(
      topic: String,
      key: Any,
      keyBytes: Array[Byte],
      value: Any,
      valueBytes: Array[Byte],
      cluster: Cluster
  ): Int = {
    key.asInstanceOf[String].toInt % 2
  }
  override def close(): Unit                                 = ()
  override def configure(configs: util.Map[String, _]): Unit = ()
}

class KafkaConsumerSpec extends AsyncIntegrationSpec with EmbeddedKafka with BeforeAndAfterAll {
  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = 20.seconds, interval = 500.millis)
  implicit val timer = IO.timer(global)
  implicit val CS    = IO.contextShift(global)
  implicit val CE    = IO.ioConcurrentEffect(CS)

  override def beforeAll(): Unit = {
    EmbeddedKafka.start()
    ()
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
    ()
  }

  val configInBatchesOf5: KafkaConsumerConfig => KafkaConsumerConfig = (cfg: KafkaConsumerConfig) =>
    cfg.copy(maybeMaxPollRecords = Some(5))

  "MyKafkaConsumer" should {
    "consumeOneByOne" when {
      "when 1000 messages are emitted, consumer should send all 1000 to the processing function" in {
        val keys      = (1 to 1000).map(_.toString)
        val topicKeys = Map(randomTopic -> keys)
        withMessages(topicKeys) {
          val receiveQueue = new ConcurrentLinkedQueue[String]()
          val processEvent: ConsumerRecord[String, String] => IO[ProcessAction] = { entry =>
            receiveQueue.offer(entry.key())
            IO.pure(ProcessAction.commitAll)
          }

          withMyKafkaConsumer(topicKeys.keySet)(processEvent) {
            IO.pure {
              eventually {
                receiveQueue.size mustBe keys.size
                ensurePartitionsOrderingExpected(receiveQueue, keys)
              }
            }
          }
        }.unsafeToFuture()
      }

      "when a message processing returns Retry, it should be retried" in {
        val keys      = (1 to 1000).map(_.toString)
        val topicKeys = Map(randomTopic -> keys)
        withMessages(topicKeys) {
          val receiveQueue = new ConcurrentLinkedQueue[String]()
          val retriedQueue = new ConcurrentLinkedQueue[String]()
          val processEvent: ConsumerRecord[String, String] => IO[ProcessAction] = { entry =>
            if (entry.key() == "80" && retriedQueue.size() < 2) {
              retriedQueue.offer(entry.key())
              IO.pure(ProcessAction.rejectAll("error"))
            } else {
              receiveQueue.offer(entry.key())
              IO.pure(ProcessAction.commitAll)
            }
          }

          withMyKafkaConsumer(topicKeys.keySet)(processEvent) {
            IO.pure {
              eventually {
                retriedQueue.size mustBe 2
                retriedQueue.asScala.toList.count(_ == "80") mustBe 2
                receiveQueue.size mustBe keys.size
                ensurePartitionsOrderingExpected(receiveQueue, keys)
              }
            }
          }
        }.unsafeToFuture()
      }

      "when a message processing returns Reject, it should be Rejected and paused, and then resumed" in {
        val keys      = (1 to 1000).map(_.toString)
        val topicKeys = Map(randomTopic -> keys)
        withMessages(topicKeys) {
          val receiveQueue  = new ConcurrentLinkedQueue[String]()
          val rejectedQueue = new ConcurrentLinkedQueue[String]()
          val processEvent: ConsumerRecord[String, String] => IO[ProcessAction] = { entry =>
            if (entry.key() == "86" && rejectedQueue.size() < 2) {
              rejectedQueue.offer(entry.key())
              IO.pure(
                ProcessAction.rejectPauseAll(PausedTemporarily(Instant.now(), 500.millis), "error")
              )
            } else {
              receiveQueue.offer(entry.key())
              IO.pure(ProcessAction.commitAll)
            }
          }

          withMyKafkaConsumer(topicKeys.keySet, configInBatchesOf5)(processEvent) {
            IO.pure {
              eventually {
                rejectedQueue.size mustBe 2
                rejectedQueue.asScala.toList.count(_ == "86") mustBe 2
                receiveQueue.size mustBe keys.size
                ensurePartitionsOrderingExpected(receiveQueue, keys)
              }
            }
          }
        }.unsafeToFuture()
      }

      "when a message processing returns Reject, it should be Rejected and topic paused, ensure no events are consumed from that partition while paused" in {
        val keys      = (1 to 1000).map(_.toString)
        val topicKeys = Map(randomTopic -> keys)
        withMessages(topicKeys) {
          val receiveQueue  = new ConcurrentLinkedQueue[String]()
          val rejectedQueue = new ConcurrentLinkedQueue[String]()
          val processEvent: ConsumerRecord[String, String] => IO[ProcessAction] = { entry =>
            if (entry.key() == "80") {
              rejectedQueue.offer(entry.key())
              IO.pure(ProcessAction.rejectPauseAll(PausedTemporarily(Instant.now, 1.hour), "error"))
            } else {
              receiveQueue.offer(entry.key())
              IO.pure(ProcessAction.commitAll)
            }
          }

          withMyKafkaConsumer(topicKeys.keySet)(processEvent) {
            Thread.sleep(10.seconds.toMillis)
            val expected = ((1 to 79) ++ (81 to 1000 by 2)).map(_.toString)
            IO.pure {
              eventually {
                rejectedQueue.size mustBe 1
                rejectedQueue.asScala.count(_ == "80") mustBe 1
                receiveQueue.size mustBe expected.size
                ensurePartitionsOrderingExpected(receiveQueue, expected)
              }
            }
          }
        }.unsafeToFuture()
      }

      "when message processing throws, consumer should be restarted and consumption reattempted" in {
        val keys      = (1 to 1000).map(_.toString)
        val topicKeys = Map(randomTopic -> keys)
        withMessages(topicKeys) {
          val receiveQueue = new ConcurrentLinkedQueue[String]()
          val failedQueue  = new ConcurrentLinkedQueue[String]()
          val processEvent: ConsumerRecord[String, String] => IO[ProcessAction] = { entry =>
            if (entry.key() == "80" && failedQueue.isEmpty) {
              failedQueue.offer(entry.key())
              IO.raiseError(new Exception("Ops"))
            } else {
              receiveQueue.offer(entry.key())
              IO.pure(ProcessAction.commitAll)
            }
          }

          withMyKafkaConsumer(topicKeys.keySet)(processEvent) {
            Thread.sleep(10.seconds.toMillis)
            IO.pure {
              eventually {
                failedQueue.size mustBe 1
                failedQueue.asScala.toList.head mustBe "80"
                receiveQueue.size mustBe keys.size
                ensurePartitionsOrderingExpected(receiveQueue, keys)
              }
            }
          }
        }.unsafeToFuture()
      }

      "when 1000 messages are emitted, seeks to beginning and consumes the lot again" in {
        val keys      = (1 to 1000).map(_.toString)
        val topicKeys = Map(randomTopic -> keys)
        val topicName = topicKeys.keySet.head.name

        withMessages(topicKeys) {
          val receiveQueue = new ConcurrentLinkedQueue[String]()

          val processEvent: ConsumerRecord[String, String] => IO[ProcessAction] = { entry =>
            receiveQueue.offer(entry.key())
            IO.pure(ProcessAction.commitAll)
          }

          usingMyKafkaConsumer(topicKeys.keySet)(processEvent) { consumer =>
            val tps =
              consumer.listTopics().map {
                _.get(topicName).toList.flatten.map { pi =>
                  new TopicPartition(pi.topic(), pi.partition())
                }
              }

            IO.sleep(10.seconds) *> tps.flatMap(consumer.seekToBeginning) *> IO.sleep(10.seconds) *> IO {
              eventually(receiveQueue.size mustBe keys.size * 2)
            }
          }

        }.unsafeToFuture()
      }
    }

    "consumeInTopicPartitionBatches" when {
      "when 1000 messages are emitted, consumer should send all 1000 to the processing function" in {
        val keys      = (1 to 1000).map(_.toString)
        val topicKeys = Map(randomTopic -> keys)
        withMessages(topicKeys) {
          val receiveQueue = new ConcurrentLinkedQueue[String]()
          val processEvent: TPRecords[String, String] => IO[ProcessAction] = { entry =>
            entry._2.map { item =>
              receiveQueue.offer(item.key())
            }
            IO.pure(ProcessAction.commitAll)
          }

          withMyKafkaConsumer[TPRecords](topicKeys.keySet, configInBatchesOf5)(processEvent) {
            IO.pure {
              eventually {
                receiveQueue.size mustBe keys.size
                ensurePartitionsOrderingExpected(receiveQueue, keys)
              }
            }
          }
        }.unsafeToFuture()
      }

      "when a message processing returns Retry, it should be retried" in {
        val keys      = (1 to 1000).map(_.toString)
        val topicKeys = Map(randomTopic -> keys)
        withMessages(topicKeys) {
          val receiveQueue = new ConcurrentLinkedQueue[String]()
          val retriedQueue = new ConcurrentLinkedQueue[TPRecords[String, String]]()
          val processEvent: TPRecords[String, String] => IO[ProcessAction] = { entry =>
            if (entry._2.exists(_.key() == "80") && retriedQueue.size() < 2) {
              retriedQueue.offer(entry)
              IO.pure(ProcessAction.rejectAll("error"))
            } else {
              entry._2.map { item =>
                receiveQueue.offer(item.key())
              }
              IO.pure(ProcessAction.commitAll)
            }
          }

          withMyKafkaConsumer[TPRecords](topicKeys.keySet, configInBatchesOf5)(processEvent) {
            IO.pure {
              eventually {
                receiveQueue.size mustBe keys.size
                ensurePartitionsOrderingExpected(receiveQueue, keys)
                retriedQueue.size() mustBe 2
                retriedQueue.asScala.forall(_._2.exists(_.key() == "80")) mustBe true
              }
            }
          }
        }.unsafeToFuture()
      }

      "when a message processing returns Reject, it should be Rejected and paused, and then resumed" in {
        val keys      = (1 to 1000).map(_.toString)
        val topicKeys = Map(randomTopic -> keys)
        withMessages(topicKeys) {
          val receiveQueue  = new ConcurrentLinkedQueue[String]()
          val rejectedQueue = new ConcurrentLinkedQueue[TPRecords[String, String]]()
          val processEvent: TPRecords[String, String] => IO[ProcessAction] = { entry =>
            if (entry._2.exists(_.key() == "84") && rejectedQueue.size() < 2) {
              rejectedQueue.offer(entry)
              IO.pure(
                ProcessAction.rejectPauseAll(PausedTemporarily(Instant.now, 500.millis), "error")
              )
            } else {
              entry._2.map { item =>
                receiveQueue.offer(item.key())
              }
              IO.pure(ProcessAction.commitAll)
            }
          }

          withMyKafkaConsumer[TPRecords](topicKeys.keySet, configInBatchesOf5)(processEvent) {
            IO.pure {
              eventually {
                receiveQueue.size mustBe keys.size
                ensurePartitionsOrderingExpected(receiveQueue, keys)
                rejectedQueue.size() mustBe 2
                rejectedQueue.asScala.forall(_._2.exists(_.key() == "84")) mustBe true
              }
            }
          }
        }.unsafeToFuture()
      }

      "when a message processing returns Reject, it should be Rejected and topic paused, ensure no events are consumed from that partition while paused" in {
        val keys      = (1 to 1000).map(_.toString)
        val topicKeys = Map(randomTopic -> keys)
        withMessages(topicKeys) {
          val receiveQueue  = new ConcurrentLinkedQueue[String]()
          val rejectedQueue = new ConcurrentLinkedQueue[TPRecords[String, String]]()
          val processEvent: TPRecords[String, String] => IO[ProcessAction] = { entry =>
            if (entry._2.exists(_.key() == "84")) {
              rejectedQueue.offer(entry)
              IO.pure(ProcessAction.rejectPauseAll(PausedTemporarily(Instant.now, 1.hour), "error"))
            } else {
              entry._2.map { item =>
                receiveQueue.offer(item.key())
              }
              IO.pure(ProcessAction.commitAll)
            }
          }

          withMyKafkaConsumer[TPRecords](topicKeys.keySet, configInBatchesOf5)(processEvent) {
            IO.pure {
              eventually {
                val minRejected = rejectedQueue.asScala.flatMap(_._2.toList).map(_.key().toInt).min
                receiveQueue.asScala.exists(e => e.toInt % 2 == 0 && e.toInt >= minRejected) mustBe false
                rejectedQueue.size() mustBe 1
                rejectedQueue.asScala.forall(_._2.exists(_.key() == "84")) mustBe true
              }
            }
          }
        }.unsafeToFuture()
      }

      "when message processing throws, consumer should be restarted and consumption reattempted" in {
        val keys      = (1 to 1000).map(_.toString)
        val topicKeys = Map(randomTopic -> keys)
        withMessages(topicKeys) {
          val receiveQueue = new ConcurrentLinkedQueue[String]()
          val failedQueue  = new ConcurrentLinkedQueue[TPRecords[String, String]]()
          val processEvent: TPRecords[String, String] => IO[ProcessAction] = { entry =>
            if (entry._2.exists(_.key() == "80") && failedQueue.isEmpty) {
              failedQueue.offer(entry)
              IO.raiseError(new Exception("Ops"))
            } else {
              entry._2.map { item =>
                receiveQueue.offer(item.key())
              }
              IO.pure(ProcessAction.commitAll)
            }
          }

          withMyKafkaConsumer[TPRecords](topicKeys.keySet, configInBatchesOf5)(processEvent) {
            Thread.sleep(10.seconds.toMillis)
            IO.pure {
              eventually {
                failedQueue.size mustBe 1
                failedQueue.asScala.headOption.exists(_._2.exists(_.key == "80")) mustBe true
                receiveQueue.size mustBe keys.size
                ensurePartitionsOrderingExpected(receiveQueue, keys)
              }
            }
          }
        }.unsafeToFuture()
      }
    }
  }

  def ensurePartitionsOrderingExpected(
      receiveQueue: ConcurrentLinkedQueue[String],
      keys: Seq[String]
  ): Assertion = {
    receiveQueue
      .iterator()
      .asScala
      .toSeq
      .filter(el => el.toInt                                    % 2 == 0) mustBe keys.filter(el => el.toInt % 2 == 0) //Ensuring order is correct
    receiveQueue.iterator().asScala.toSeq.filter(el => el.toInt % 2 == 1) mustBe keys.filter(el =>
      el.toInt                                                  % 2 == 1
    )
  }

  def usingMyKafkaConsumer[G[_, _]: KafkaProcessable](
      topics: Set[TopicDefinitionDetails],
      configMutations: KafkaConsumerConfig => KafkaConsumerConfig = identity
  )(
      processEvent: G[String, String] => IO[ProcessAction]
  )(block: KafkaConsumerIO[IO, String, String] => IO[Assertion]): IO[Assertion] = {
    constructConsumer[IO](topics, configMutations).use { consumer =>
      val consumerLoop = consumer.pollForever
        .consuming[G]
        .expectingProcessAction
        .run(10.millis)(processEvent)

      for {
        cancelable <- consumerLoop
        b          <- block(consumer).recoverWith { case e => cancelable.flatMap(_ => IO.raiseError(e)) }
        _          <- cancelable
      } yield b
    }
  }

  def withMyKafkaConsumer[G[_, _]: KafkaProcessable](
      topics: Set[TopicDefinitionDetails],
      configMutations: KafkaConsumerConfig => KafkaConsumerConfig = identity
  )(processEvent: G[String, String] => IO[ProcessAction])(block: => IO[Assertion]): IO[Assertion] =
    usingMyKafkaConsumer(topics, configMutations)(processEvent)(_ => block)

  def withMessages[T](
      topicAndKeys: Map[TopicDefinitionDetails, Seq[String]]
  )(block: => IO[T]): IO[T] = {
    implicit val CS = IO.contextShift(global)
    implicit val CE = IO.ioConcurrentEffect(CS)
    withTopics(topicAndKeys.keySet) { _ =>
      constructProducer[IO](classOf[TestPartitioner2Partitions]).use { producer =>
        topicAndKeys.toList.traverse {
          case (topic, keys) =>
            fillTopic[IO, String, String](
              producer,
              topic.name,
              keys.map(_ -> UUID.randomUUID.toString)
            )
        }.flatMap(_ => block)
      }
    }
  }
}
