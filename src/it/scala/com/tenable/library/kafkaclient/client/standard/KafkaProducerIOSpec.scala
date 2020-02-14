package com.tenable.library.kafkaclient.client.standard

import cats.effect.IO
import com.tenable.library.kafkaclient.testhelpers.{GeneralKafkaHelpers, SyncIntegrationSpec}
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.PartitionInfo

import scala.concurrent.ExecutionContext
import com.tenable.library.kafkaclient.config.TopicDefinitionDetails
import net.manub.embeddedkafka.EmbeddedKafka
import org.scalatest.BeforeAndAfterAll

class KafkaProducerIOSpec extends SyncIntegrationSpec with EmbeddedKafka with BeforeAndAfterAll {
  import GeneralKafkaHelpers._
  implicit val CS = IO.contextShift(ExecutionContext.global)
  implicit val CE = IO.ioConcurrentEffect(CS)

  override def beforeAll(): Unit = {
    EmbeddedKafka.start()
    ()
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
    ()
  }

  "ProducerIO" should {
    "partitionsFor" when {
      "listing partitionsFor a topic" in {
        val topic: TopicDefinitionDetails = randomTopic

        withProducer[IO, Seq[PartitionInfo]](_.partitionsFor(topic.name)).map { partitions =>
          partitions.head.topic mustBe topic.name
          partitions.head.partition mustBe 0
          partitions.head.replicas.length mustBe 1
          partitions.head.inSyncReplicas.length mustBe 1
        }
      }
    }

    "forTopic" when {
      "sending a message for topic" in {
        val topic: TopicDefinitionDetails = randomTopic
        val myKey                         = "some-key"
        val myValue                       = "some-value"

        withProducer[IO, RecordMetadata](_.forTopic[String](topic.name).send(myKey, myValue)).map {
          record =>
            record.topic mustBe topic.name
            record.offset mustBe 0
            record.serializedKeySize mustBe myKey.length
            record.serializedValueSize mustBe myValue.length
        }
      }
    }
  }
}
