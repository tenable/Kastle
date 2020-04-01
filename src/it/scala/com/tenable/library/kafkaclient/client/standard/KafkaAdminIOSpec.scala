package com.tenable.library.kafkaclient.client.standard

import cats.effect.IO
import com.github.ghik.silencer.silent
import com.tenable.library.kafkaclient.testhelpers.{GeneralKafkaHelpers, SyncIntegrationSpec}
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.kafka.clients.admin.{NewPartitions, TopicListing}
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt
import scala.util.Random

@silent
class KafkaAdminIOSpec extends SyncIntegrationSpec with EmbeddedKafka with BeforeAndAfterAll {
  import GeneralKafkaHelpers._
  implicit val timer = IO.timer(ExecutionContext.global)
  implicit val CS    = IO.contextShift(ExecutionContext.global)

  override def beforeAll(): Unit = {
    EmbeddedKafka.start()
    ()
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
    ()
  }

  "KafkaAdminIO" should {
    "create, delete, list topics" when {
      "create a topic and then ensure it exists" in {
        val topics = randomTopics()

        val task = withAdmin { admin: KafkaAdminIO[IO] =>
          cleanupTopicsAfter(admin) {
            for {
              _               <- admin.createTopics(topics)
              describedTopics <- admin.describeTopics(topics.map(_.name))
              listedTopics    <- filteredTopicsString(admin)
            } yield {
              listedTopics.toSet mustBe topics.map(_.name)

              describedTopics.keys.toSet mustEqual topics.map(_.name)
              describedTopics.map { case (k, v) => k -> v.partitions().size() } mustEqual
                topics.map(t => t.name -> t.partitions).toMap
            }
          }
        }

        task.unsafeRunSync
      }

      "create a topic and then delete it" in {
        val topics = randomTopics()

        val task = withAdmin { admin: KafkaAdminIO[IO] =>
          cleanupTopicsAfter(admin) {
            for {
              _            <- admin.createTopics(topics)
              topicsBefore <- filteredTopicsString(admin)
              _            <- admin.deleteTopics(topics.map(t => t.name))
              topicsAfter  <- filteredTopicsString(admin)
            } yield {
              topicsBefore.toSet mustBe topics.map(_.name)
              topicsAfter.toSet mustBe Set.empty[String]
            }
          }
        }

        task.unsafeRunSync
      }
    }

    "create partitions for given topics" when {
      "topic created and partitions added" in {
        val topics = randomTopics(count = 1)

        val task = withAdmin { admin: KafkaAdminIO[IO] =>
          cleanupTopicsAfter(admin) {
            for {
              _                    <- admin.createTopics(topics)
              describedTopicBefore <- admin.describeTopics(topics.map(_.name)).map(_.values.head)
              partitionsAdjustment = Map(describedTopicBefore.name -> NewPartitions.increaseTo(10))
              _                   <- admin.createPartitions(partitionsAdjustment)
              describedTopicAfter <- admin.describeTopics(topics.map(_.name)).map(_.values.head)
            } yield {
              describedTopicBefore.partitions.size mustBe 2
              describedTopicAfter.partitions.size mustBe 10
            }
          }
        }

        task.unsafeRunSync
      }
    }

    "handle consumer groups by" when {
      "listing" in {
        val topics = randomTopics(count = 1)
        val groupId =
          s"unit.${Random.alphanumeric.filter(_.isLetter).take(4).mkString.toLowerCase}.1"

        val task = withAdmin { admin: KafkaAdminIO[IO] =>
          cleanupTopicsAfter(admin) {
            constructConsumer[IO](topics, _.copy(groupId = groupId)).use {
              consumer: KafkaConsumerIO[IO, String, String] =>
                for {
                  _              <- consumer.poll(1.seconds)(identity)
                  consumerGroups <- admin.listConsumerGroups()
                } yield {
                  consumerGroups.head.groupId mustBe groupId
                }
            }
          }

        }

        task.unsafeRunSync
      }

      "describing" in {
        val topics = randomTopics(count = 1)
        val groupId =
          s"unit.${Random.alphanumeric.filter(_.isLetter).take(4).mkString.toLowerCase}.1"

        val task = withAdmin { admin: KafkaAdminIO[IO] =>
          cleanupTopicsAfter(admin) {
            constructConsumer[IO](topics, _.copy(groupId = groupId)).use {
              consumer: KafkaConsumerIO[IO, String, String] =>
                for {
                  _              <- consumer.poll(1.seconds)(identity)
                  consumerGroups <- admin.describeConsumerGroups(List(groupId))
                } yield {
                  val groupDetails = consumerGroups.values.head
                  groupDetails.groupId mustBe groupId
                  // Could test consumer client id here, but not available
                }
            }
          }
        }

        task.unsafeRunSync
      }

      "getting offsets" in {
        import scala.collection.JavaConverters._

        val topics = randomTopics(count = 1)
        val groupId =
          s"unit.${Random.alphanumeric.filter(_.isLetter).take(4).mkString.toLowerCase}.1"

        val task = withAdmin { admin: KafkaAdminIO[IO] =>
          cleanupTopicsAfter(admin) {
            constructConsumer[IO](topics, _.copy(groupId = groupId)).use {
              consumer: KafkaConsumerIO[IO, String, String] =>
                withProducerIO { producer: KafkaProducerIO[IO, String, String] =>
                  for {
                    sent   <- producer.send(topics.head.name, "some-key", "some-value")
                    record <- consumer.poll(10.seconds)(identity)
                    commit = record.partitions.asScala
                      .map(tps => tps -> (record.records(tps).asScala.last.offset + 1))
                    _                    <- consumer.commitSync(commit.toMap)
                    consumerGroupsOffset <- admin.listConsumerGroupOffsets(groupId)
                  } yield {
                    sent.topic mustBe topics.head.name
                    record.records(topics.head.name).asScala.head.value mustBe "some-value"
                    consumerGroupsOffset.values.head.offset mustBe 1L
                  }
                }
            }
          }
        }

        task.unsafeRunSync
      }

      "getting many offsets" in {
        import scala.collection.JavaConverters._

        val topics = randomTopics(count = 1)
        val groupId =
          s"unit.${Random.alphanumeric.filter(_.isLetter).take(4).mkString.toLowerCase}.1"

        val task = withAdmin { admin: KafkaAdminIO[IO] =>
          cleanupTopicsAfter(admin) {
            constructConsumer[IO](topics, _.copy(groupId = groupId)).use {
              consumer: KafkaConsumerIO[IO, String, String] =>
                withProducerIO { producer: KafkaProducerIO[IO, String, String] =>
                  for {
                    _       <- producer.send(topics.head.name, "some-key", "some-value1")
                    record1 <- consumer.poll(10.seconds)(identity)
                    commit1 = record1.partitions.asScala
                      .map(tps => tps -> (record1.records(tps).asScala.last.offset + 1))
                    _ <- consumer.commitSync(commit1.toMap)

                    _       <- producer.send(topics.head.name, "some-key", "some-value2")
                    record2 <- consumer.poll(10.seconds)(identity)
                    commit2 = record2.partitions.asScala
                      .map(tps => tps -> (record2.records(tps).asScala.last.offset + 1))
                    _ <- consumer.commitSync(commit2.toMap)

                    _       <- producer.send(topics.head.name, "some-key", "some-value3")
                    record3 <- consumer.poll(10.seconds)(identity)
                    commit3 = record3.partitions.asScala
                      .map(tps => tps -> (record3.records(tps).asScala.last.offset + 1))
                    _ <- consumer.commitSync(commit3.toMap)

                    consumerGroupsOffset <- admin.listConsumerGroupOffsets(groupId)
                  } yield {
                    consumerGroupsOffset.values.head.offset mustBe 3L
                  }
                }
            }
          }
        }

        task.unsafeRunSync
      }
    }
  }

  private def cleanupTopicsAfter[R](admin: KafkaAdminIO[IO])(testCase: => IO[R]): IO[R] =
    for {
      retval   <- testCase
      toDelete <- filteredTopicsTopicName(admin)
      _        <- admin.deleteTopics(toDelete.toSet)
    } yield retval

  private def filteredTopics(admin: KafkaAdminIO[IO]): IO[List[TopicListing]] =
    admin.listTopics().map(_.filterNot(_.name.startsWith("__")))

  private def filteredTopicsString(admin: KafkaAdminIO[IO]): IO[List[String]] =
    filteredTopics(admin).map(_.map(_.name))

  private def filteredTopicsTopicName(admin: KafkaAdminIO[IO]): IO[List[String]] =
    filteredTopicsString(admin)
}
