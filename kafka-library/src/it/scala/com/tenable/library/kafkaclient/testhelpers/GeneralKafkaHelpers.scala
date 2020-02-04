package com.tenable.library.kafkaclient.testhelpers

import java.util.UUID

import cats.effect._
import cats.instances.list._
import cats.syntax.applicativeError._
import cats.syntax.flatMap._
import cats.syntax.apply._
import cats.syntax.functor._
import cats.syntax.traverse._
import com.tenable.library.kafkaclient.client.standard.{
  KafkaAdminIO,
  KafkaConsumerIO,
  KafkaProducerIO
}
import com.tenable.library.kafkaclient.config._
import org.apache.kafka.clients.producer.{Partitioner, ProducerConfig}
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.common.serialization.Serializer
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.util.Random
import scala.collection.JavaConverters._
import org.apache.kafka.clients.admin.TopicDescription

object GeneralKafkaHelpers {
  lazy val defaultKeySerde   = new StringSerde()
  lazy val defaultValueSerde = new StringSerde()

  private lazy val logger                 = LoggerFactory.getLogger(getClass)
  lazy implicit val kafkaConnectionString = brokers.mkString(",")
  lazy val brokers: Seq[String]           = Seq("127.0.0.1:29092")
  lazy val adminConfig: KafkaAdminConfig  = KafkaAdminConfig.default(kafkaConnectionString)

  def withAdmin[F[_]: Concurrent: ContextShift, A](f: KafkaAdminIO[F] => F[A]): F[A] =
    KafkaAdminIO
      .builder[F](adminConfig)
      .resource
      .use(f)

  def randomTopic: TopicDefinitionDetails =
    TopicDefinitionDetails(
      s"prefix.pub.ittests.${Random.alphanumeric.filter(_.isLetter).take(10).mkString.toLowerCase}.1",
      2,
      1.toShort
    )

  def randomTopics(count: Int = 5): Set[TopicDefinitionDetails] =
    Seq
      .fill[TopicDefinitionDetails](count)(randomTopic)
      .toSet

  def createTopics[F[_]: Concurrent: ContextShift](
      topicDefinitions: Set[TopicDefinitionDetails]
  ): F[Unit] = withAdmin { admin: KafkaAdminIO[F] =>
    admin
      .createTopics(topicDefinitions)
  }

  def createTopics[F[_]: Concurrent: ContextShift](
      topicNames: Set[String],
      partitions: Int
  ): F[Unit] =
    createTopics(topicNames.map(tn => new TopicDefinitionDetails(tn, partitions, 1.toShort)))

  def randomConsumerGroup: String = {
    s"prefix.${Random.alphanumeric.filter(_.isLetter).take(10).mkString.toLowerCase}.1"
  }

  def fillTopic[F[_]: Concurrent: ContextShift, K, V](
      topicName: String,
      messages: Seq[(K, V)],
      keySerializer: Serializer[K],
      valueSerializer: Serializer[V]
  ): F[Unit] = {
    val config = KafkaProducerConfig(
      connectionString = kafkaConnectionString,
      ackStrategy = AckStrategy.All,
      retries = 1,
      batchSize = 1,
      linger = 0.millis,
      bufferSize = 1024L * 1024L,
      additionalProperties = Map.empty[String, String]
    )
    KafkaProducerIO
      .builder(config)
      .withKeyDeserializer(keySerializer)
      .withValueDeserializer(valueSerializer)
      .resource
      .use(fillTopic[F, K, V](_, topicName, messages))
  }

  def withProducer[F[_]: Concurrent: ContextShift, B](
      f: KafkaProducerIO[F, String, String] => F[B]
  ): F[B] = {
    val config = KafkaProducerConfig(
      connectionString = kafkaConnectionString,
      ackStrategy = AckStrategy.All,
      retries = 1,
      batchSize = 1,
      linger = 0.millis,
      bufferSize = 1024L * 1024L,
      additionalProperties = Map.empty[String, String]
    )
    KafkaProducerIO
      .builder(config)
      .withKeyDeserializer(defaultKeySerde.serializer)
      .withValueDeserializer(defaultValueSerde.serializer)
      .resource
      .use(producer => f(producer))
  }

  def fillTopic[F[_]: Async, K, V](
      producer: KafkaProducerIO[F, K, V],
      topicName: String,
      messages: Seq[(K, V)]
  ): F[Unit] =
    for {
      _ <- Async[F].delay {
            logger.info(s"filling topic $topicName with ${messages.length} messages")
          }
      _ <- messages.toList.traverse {
            case (key, value) =>
              producer.send(topicName, key, value)
          }
      _ = logger.info(s"successfully filled topic $topicName with ${messages.length} messages")
    } yield ()

  def withTopic[F[_]: Concurrent: ContextShift, T](
      topic: TopicDefinitionDetails
  )(task: TopicDefinitionDetails => F[T]): F[T] =
    withTopics(Set(topic))(topics => task(topics.head)) //TODO !!!

  def withTopics[F[_]: Concurrent: ContextShift, T](
      topics: Set[TopicDefinitionDetails]
  )(task: Set[TopicDefinitionDetails] => F[T]): F[T] = withAdmin { admin: KafkaAdminIO[F] =>
    def findInvalid(input: Map[String, TopicDescription]): List[String] = {
      input.toList.map {
        case (t, td) => (t, td, topics.find(_.name == t))
      }.collect {
        case (t, _, None) =>
          s"Missing $t, probably creation failed"
        case (t, td, Some(src))
            if td.partitions.size != src.partitions || td.partitions.asScala
              .map(_.replicas.size())
              .min != src.replicationFactor.toInt =>
          s"Settings for $t mistmatch, expected $src"
      }

    }
    val wrappedTask =
      for {
        ts <- admin.describeTopics(topics.map(_.name))
        toCreate = topics.filterNot(tdd => ts.keySet(tdd.name))
        _           <- if (toCreate.nonEmpty) admin.createTopics(toCreate) else Concurrent[F].unit
        afterCreate <- admin.describeTopics(topics.map(_.name))
        invalid = findInvalid(afterCreate)
        _ <- if (invalid.nonEmpty) {
              Concurrent[F].raiseError[Unit](
                new Exception(s"Failed to create topics, reasons: ${invalid.mkString("\n")}")
              )
            } else Concurrent[F].unit
        result <- task(topics)
      } yield result

    wrappedTask <*
      admin
        .deleteTopics(topics.map(_.name))
        .handleError { e =>
          logger.warn(
            s"Error deleting topics.  This may be nothing to worry about (for example if they were never created).",
            e
          )
        }
  }

  val defaultDummyMessageCount = 5

  def withDummyMessage[F[_]: Concurrent: ContextShift, T](
      topic: TopicDefinitionDetails,
      count: Int = defaultDummyMessageCount
  )(block: Seq[(String, String)] => F[T]): F[T] =
    fillTopicWithDummyMessages(topic, count).flatMap(block)

  def withDummyMessages[F[_]: Concurrent: ContextShift, T](
      topics: Set[TopicDefinitionDetails],
      count: Int = defaultDummyMessageCount
  )(block: Map[TopicDefinitionDetails, Seq[(String, String)]] => F[T]): F[T] =
    topics.toList.traverse { topic =>
      withDummyMessage(topic, count)(msgs => Concurrent[F].pure(topic -> msgs))
    }.map(_.toMap)
      .flatMap(block)

  def withFilledTopic[F[_]: Concurrent: ContextShift, T](
      topic: TopicDefinitionDetails,
      msgCount: Int = defaultDummyMessageCount
  )(block: (TopicDefinitionDetails, Seq[(String, String)]) => F[T]): F[T] =
    withTopic(topic) { topic =>
      withDummyMessage(topic, msgCount) { messages =>
        block(topic, messages)
      }
    }

  def withFilledTopics[F[_]: Concurrent: ContextShift, T](
      topics: Set[TopicDefinitionDetails],
      msgCount: Int = defaultDummyMessageCount
  )(
      block: (
          Set[TopicDefinitionDetails],
          Map[TopicDefinitionDetails, Seq[(String, String)]]
      ) => F[T]
  ): F[T] =
    withTopics(topics) { topics =>
      withDummyMessages(topics, msgCount) { messages =>
        block(topics, messages)
      }
    }

  def fillTopicWithDummyMessages[F[_]: Concurrent: ContextShift](
      topic: TopicDefinitionDetails,
      count: Int = defaultDummyMessageCount
  ): F[Seq[(String, String)]] = {
    val messages = Seq.fill(count)(UUID.randomUUID().toString -> UUID.randomUUID().toString)
    fillTopicWithMessages(topic, messages)
  }

  def fillTopicWithMessages[F[_]: Concurrent: ContextShift](
      topic: TopicDefinitionDetails,
      messages: Seq[(String, String)]
  ): F[Seq[(String, String)]] = {
    fillTopic[F, String, String](
      topic.name,
      messages,
      defaultKeySerde.serializer(),
      defaultValueSerde.serializer()
    ).map(_ => messages)
  }

  def constructConsumerConfig(
      topics: Set[TopicDefinitionDetails],
      maxPollInterval: FiniteDuration
  ): KafkaConsumerConfig =
    KafkaConsumerConfig(
      connectionString = kafkaConnectionString,
      topics = topics.map(_.name),
      groupId = s"prefix.${Random.alphanumeric.filter(_.isLetter).take(10).mkString.toLowerCase}.1",
      maxPollInterval = maxPollInterval
    )

  def constructConsumer[F[_]: ConcurrentEffect: ContextShift: Timer](
      topics: Set[TopicDefinitionDetails],
      configMutations: KafkaConsumerConfig => KafkaConsumerConfig = identity
  ): Resource[F, KafkaConsumerIO[F, String, String]] =
    KafkaConsumerIO
      .builder(configMutations(constructConsumerConfig(topics, 10.seconds)))
      .withKeyDeserializer(defaultKeySerde.deserializer())
      .withValueDeserializer(defaultValueSerde.deserializer())
      .resource

  def constructProducer[F[_]: Concurrent: ContextShift](
      partitioner: Class[_ <: Partitioner]
  ): Resource[F, KafkaProducerIO[F, String, String]] = {
    val config = KafkaProducerConfig(
      connectionString = kafkaConnectionString,
      ackStrategy = AckStrategy.All,
      retries = 1,
      batchSize = 16384,
      linger = 0.millis,
      bufferSize = 1024L * 1024L,
      additionalProperties = Map(
        ProducerConfig.PARTITIONER_CLASS_CONFIG -> partitioner
      )
    )
    KafkaProducerIO
      .builder(config)
      .withKeyDeserializer(defaultKeySerde.serializer)
      .withValueDeserializer(defaultValueSerde.serializer)
      .resource
  }
}
