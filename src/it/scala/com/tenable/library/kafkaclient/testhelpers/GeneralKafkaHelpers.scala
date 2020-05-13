package com.tenable.library.kafkaclient.testhelpers

import java.util.UUID

import cats.effect._
import cats.instances.list._
import cats.syntax.applicativeError._
import cats.syntax.flatMap._
import cats.syntax.apply._
import cats.syntax.either._
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
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import com.tenable.library.kafkaclient.client.standard.consumer.ExternalOffsetRebalanceListener
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener

object GeneralKafkaHelpers {
  lazy val defaultKeySerde   = new StringSerde()
  lazy val defaultValueSerde = new StringSerde()

  private lazy val logger                         = LoggerFactory.getLogger(getClass)
  def adminConfig(conn: String): KafkaAdminConfig = KafkaAdminConfig.default(conn)

  def withAdmin[F[_]: Concurrent: ContextShift, A](
      f: KafkaAdminIO[F] => F[A]
  )(implicit C: EmbeddedKafkaConfig): F[A] =
    KafkaAdminIO
      .builder[F](adminConfig(s"localhost:${C.kafkaPort}"))
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
  )(implicit C: EmbeddedKafkaConfig): F[Unit] = {
    val config = KafkaProducerConfig(
      connectionString = s"localhost:${C.kafkaPort}",
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

  def withProducerIO[F[_]: Concurrent: ContextShift, B](
      f: KafkaProducerIO[F, String, String] => F[B]
  )(implicit C: EmbeddedKafkaConfig): F[B] = {
    val config = KafkaProducerConfig(
      connectionString = s"localhost:${C.kafkaPort}",
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

  def ensureTopicsExist[F[_]: Async: ContextShift](
      kafkaAdminIO: KafkaAdminIO[F],
      topicDefinitions: Set[TopicDefinitionDetails]
  ): F[Either[String, Unit]] = {
    val F              = Async[F]
    val expectedTopics = topicDefinitions.map(t => t.name -> t).toMap

    //attempt to recreate all topics... if they already exist, attempt will fail harmlessly (?)
    F.delay(logger.info(s"Ensuring topics exist: ${expectedTopics.values.mkString(", ")}")) *>
      kafkaAdminIO.createTopics(topicDefinitions).attempt.flatMap { createResult =>
        logger.info(s"Created: $createResult")
        kafkaAdminIO
          .describeTopics(topicDefinitions.map(_.name))
          .handleErrorWith { e =>
            createResult.left.foreach { createException =>
              //it's normal for topic creation to fail in some cases -- only show the error
              //when all the topics we expect to exist aren't there, because this is the only case where it's likely
              //to indicate a real problem
              logger.warn(s"Topic creation failed with exception.", createException)
            }

            logger.error(
              s"Unable to list topics.  This usually means topic creation failed.  Scroll up in the log.",
              e
            )
            F.raiseError(
              new Exception(
                "Unable to list topics.  This usually means topic creation failed.  Check logs",
                e
              )
            )
          }
          .map { actualTopics =>
            logger.info(s"Actual: $actualTopics")
            val missingTopics = expectedTopics.keySet.diff(actualTopics.keySet)
            if (missingTopics.nonEmpty) {
              s"Unable to create the following topics: $missingTopics".asLeft[Unit]
            } else {
              actualTopics.collect {
                case (name, desc) if expectedTopics(name).partitions != desc.partitions().size() =>
                  s"$name partition count does not match: ${expectedTopics(name).partitions} != ${desc.partitions().size()}"
              } match {
                case errors if errors.nonEmpty => errors.mkString(",").asLeft[Unit]
                case _                         => ().asRight[String]
              }
            }
          }
      }
  }

  def withTopics[F[_]: Concurrent: ContextShift, T](
      topics: Set[TopicDefinitionDetails]
  )(task: Set[TopicDefinitionDetails] => F[T]): F[T] = withAdmin { admin: KafkaAdminIO[F] =>
    val wrappedTask =
      for {
        _      <- ensureTopicsExist(admin, topics)
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
  )(implicit C: EmbeddedKafkaConfig): KafkaConsumerConfig =
    KafkaConsumerConfig(
      connectionString = s"localhost:${C.kafkaPort}",
      topics = topics.map(_.name),
      groupId = s"prefix.${Random.alphanumeric.filter(_.isLetter).take(10).mkString.toLowerCase}.1",
      maxPollInterval = maxPollInterval
    )

  def constructConsumer[F[_]: ConcurrentEffect: ContextShift: Timer](
      topics: Set[TopicDefinitionDetails],
      rebalanceListener: Option[KafkaConsumer[_, _] => ExternalOffsetRebalanceListener[F]] = None,
      configMutations: KafkaConsumerConfig => KafkaConsumerConfig = identity
  )(implicit C: EmbeddedKafkaConfig): Resource[F, KafkaConsumerIO[F, String, String]] = {
    KafkaConsumerIO
      .builder(configMutations(constructConsumerConfig(topics, 10.seconds)))
      .withKeyDeserializer(defaultKeySerde.deserializer())
      .withValueDeserializer(defaultValueSerde.deserializer())
      .rebalanceListener(
        rebalanceListener.getOrElse((_: KafkaConsumer[_, _]) => new NoOpConsumerRebalanceListener)
      )
      .resource
  }

  def constructProducer[F[_]: Concurrent: ContextShift](
      partitioner: Class[_ <: Partitioner]
  )(implicit C: EmbeddedKafkaConfig): Resource[F, KafkaProducerIO[F, String, String]] = {
    val config = KafkaProducerConfig(
      connectionString = s"localhost:${C.kafkaPort}",
      ackStrategy = AckStrategy.All,
      retries = 1,
      batchSize = 16384,
      linger = 0.millis,
      bufferSize = 1024L * 1024L,
      additionalProperties = Map(
        ProducerConfig.PARTITIONER_CLASS_CONFIG -> partitioner.getName()
      )
    )
    KafkaProducerIO
      .builder(config)
      .withKeyDeserializer(defaultKeySerde.serializer)
      .withValueDeserializer(defaultValueSerde.serializer)
      .resource
  }
}
