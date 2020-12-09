package com.tenable.library.kafkaclient.client.standard

import java.util.concurrent.TimeUnit

import cats.effect._
import cats.implicits._
import com.tenable.library.kafkaclient.utils.ExecutionContexts
import com.tenable.library.kafkaclient.config.KafkaAdminConfig
import com.tenable.library.kafkaclient.config.TopicDefinitionDetails
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.security.token.delegation.DelegationToken

import scala.jdk.CollectionConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, DurationInt}

trait KafkaAdminIO[F[_]] {
  def createTopics(topicDefinitions: Set[TopicDefinitionDetails]): F[Unit]
  def deleteTopics(topicNames: Set[String]): F[Unit]
  def describeTopics(topicDefinitions: Set[String]): F[Map[String, TopicDescription]]
  def listTopics(): F[List[TopicListing]]
  def createPartitions(newPartitions: Map[String, NewPartitions]): F[Unit]
  def deleteRecords(recordsToDelete: Map[TopicPartition, RecordsToDelete]): F[Unit]
  def createDelegationToken(): F[DelegationToken]
  def renewDelegationToken(hmac: Array[Byte]): F[Long]
  def expireDelegationToken(hmac: Array[Byte]): F[Long]
  def describeDelegationToken(): F[List[DelegationToken]]
  def describeConsumerGroups(groupIds: List[String]): F[Map[String, ConsumerGroupDescription]]
  def listConsumerGroups(): F[List[ConsumerGroupListing]]
  def listConsumerGroupOffsets(groupId: String): F[Map[TopicPartition, OffsetAndMetadata]]
  def deleteConsumerGroups(groupIds: List[String]): F[Unit]
  def describeConfigsForTopic(resourceNames: List[String]): F[Map[String, Map[String, String]]]
  def close(): F[Unit]
  def close(timeout: Duration): F[Unit]

  // TODO other remaining methods to expose?
  //def describeCluster()
  //def describeAcls()
  //def createAcls()
  //def deleteAcls()
  //def alterConfigs()
  //def alterReplicaLogDirs()
  //def describeLogDirs()
  //def describeReplicaLogDirs()
}

object KafkaAdminIO {
  private lazy val DefaultTimeout = 10.seconds

  sealed trait BuilderState
  sealed trait CreatedEmpty extends BuilderState

  type Initialized = CreatedEmpty

  class Builder[T <: BuilderState, F[_]: Async: ContextShift] private[KafkaAdminIO] (
      config: KafkaAdminConfig,
      blockingEC: Option[Resource[F, ExecutionContext]],
      timeout: Option[Duration]
  ) {
    type OngoingBuilder[TT <: BuilderState] = Builder[TT, F]

    def withBlockingEC(blockingEC: Resource[F, ExecutionContext]): OngoingBuilder[T] =
      new Builder[T, F](
        config,
        Some(blockingEC),
        None
      )

    def withTimeout(timeout: Duration): OngoingBuilder[T] =
      new Builder[T, F](
        config,
        blockingEC,
        Some(timeout)
      )

    def resource(implicit ev: Initialized =:= T): Resource[F, KafkaAdminIO[F]] = {
      val _ = ev //shutup compiler
      KafkaAdminIO.resource(
        config,
        blockingEC,
        timeout
      )
    }
  }

  def builder[F[_]: Async: ContextShift](
      adminConfig: KafkaAdminConfig
  ): Builder[CreatedEmpty, F] =
    new Builder[CreatedEmpty, F](adminConfig, None, None)

  private def resource[F[_]: Async: ContextShift](
      adminConfig: KafkaAdminConfig,
      optionalBlockingEC: Option[Resource[F, ExecutionContext]],
      timeout: Option[Duration]
  ): Resource[F, KafkaAdminIO[F]] =
    for {
      blockingEC <- optionalBlockingEC.getOrElse(ExecutionContexts.io("kafka-admin-io"))
      admin <- Resource.make(
                 create(
                   adminConfig,
                   blockingEC,
                   timeout.getOrElse(DefaultTimeout)
                 )
               )(_.close())
    } yield admin

  private def create[F[_]: Async: ContextShift](
      adminConfig: KafkaAdminConfig,
      blockingEC: ExecutionContext,
      timeout: Duration
  ): F[KafkaAdminIO[F]] =
    Async[F].delay {
      val admin = AdminClient.create(adminConfig.properties)
      apply(admin, blockingEC, timeout)
    }

  // scalastyle:off method.length
  private def apply[F[_]: Async: ContextShift](
      admin: AdminClient,
      blockingEC: ExecutionContext,
      timeout: Duration
  ): KafkaAdminIO[F] =
    new KafkaAdminIO[F] {
      private val F  = Async[F]
      private val CS = ContextShift[F]

      override def createTopics(topicDefinitions: Set[TopicDefinitionDetails]): F[Unit] =
        CS.evalOn(blockingEC) {
          val topics = topicDefinitions
            .map(td =>
              new NewTopic(td.name, td.partitions, td.replicationFactor)
                .configs(td.properties.asJava)
            )
            .asJava

          F.delay(admin.createTopics(topics).all.get(timeout.toMillis, TimeUnit.MILLISECONDS))
            .map(_ => ())
        }

      override def deleteTopics(topicNames: Set[String]): F[Unit] =
        CS.evalOn(blockingEC) {
          val topics = topicNames.asJava

          F.delay(admin.deleteTopics(topics).all.get(timeout.toMillis, TimeUnit.MILLISECONDS))
            .map(_ => ())
        }

      override def describeTopics(
          topicDefinitions: Set[String]
      ): F[Map[String, TopicDescription]] =
        CS.evalOn(blockingEC) {
          val topics = topicDefinitions.asJava

          F.delay(admin.describeTopics(topics).all.get(timeout.toMillis, TimeUnit.MILLISECONDS))
            .map(_.asScala.toMap)
        }

      override def listTopics(): F[List[TopicListing]] =
        CS.evalOn(blockingEC) {
          F.delay(admin.listTopics.listings.get(timeout.toMillis, TimeUnit.MILLISECONDS))
            .map(_.asScala.toList)
        }

      override def createPartitions(
          newPartitions: Map[String, NewPartitions]
      ): F[Unit] =
        CS.evalOn(blockingEC) {
          val partitions =
            newPartitions.asJava

          F.delay(
            admin.createPartitions(partitions).all.get(timeout.toMillis, TimeUnit.MILLISECONDS)
          ).map(_ => ())
        }

      override def deleteRecords(
          recordsToDelete: Map[TopicPartition, RecordsToDelete]
      ): F[Unit] =
        CS.evalOn(blockingEC) {
          val records = recordsToDelete.asJava

          F.delay(admin.deleteRecords(records).all.get(timeout.toMillis, TimeUnit.MILLISECONDS))
            .map(_ => ())
        }

      override def createDelegationToken(): F[DelegationToken] =
        CS.evalOn(blockingEC) {
          F.delay(
            admin.createDelegationToken.delegationToken.get(timeout.toMillis, TimeUnit.MILLISECONDS)
          )
        }

      override def renewDelegationToken(hmac: Array[Byte]): F[Long] =
        CS.evalOn(blockingEC) {
          F.delay(admin.renewDelegationToken(hmac).expiryTimestamp.get)
        }

      override def expireDelegationToken(hmac: Array[Byte]): F[Long] =
        CS.evalOn(blockingEC) {
          F.delay(admin.expireDelegationToken(hmac).expiryTimestamp.get)
        }

      override def describeDelegationToken(): F[List[DelegationToken]] =
        CS.evalOn(blockingEC) {
          F.delay(
            admin.describeDelegationToken.delegationTokens
              .get(timeout.toMillis, TimeUnit.MILLISECONDS)
          ).map(_.asScala.toList)
        }

      override def describeConsumerGroups(
          groupIds: List[String]
      ): F[Map[String, ConsumerGroupDescription]] =
        CS.evalOn(blockingEC) {
          val groups =
            groupIds.asJava

          F.delay(
            admin.describeConsumerGroups(groups).all.get(timeout.toMillis, TimeUnit.MILLISECONDS)
          ).map(_.asScala.toMap)
        }

      override def listConsumerGroups(): F[List[ConsumerGroupListing]] =
        CS.evalOn(blockingEC) {
          F.delay(admin.listConsumerGroups.all.get(timeout.toMillis, TimeUnit.MILLISECONDS))
            .map(_.asScala.toList)
        }

      override def listConsumerGroupOffsets(
          groupId: String
      ): F[Map[TopicPartition, OffsetAndMetadata]] =
        CS.evalOn(blockingEC) {
          F.delay(
            admin
              .listConsumerGroupOffsets(groupId)
              .partitionsToOffsetAndMetadata
              .get(timeout.toMillis, TimeUnit.MILLISECONDS)
          ).map(_.asScala.toMap)
        }

      override def deleteConsumerGroups(groupIds: List[String]): F[Unit] =
        CS.evalOn(blockingEC) {
          val groups =
            groupIds.asJava

          F.delay(
            admin.deleteConsumerGroups(groups).all.get(timeout.toMillis, TimeUnit.MILLISECONDS)
          ).map(_ => ())
        }

      override def describeConfigsForTopic(
          resourceNames: List[String]
      ): F[Map[String, Map[String, String]]] =
        CS.evalOn(blockingEC) {
          val configResources = resourceNames
            .map(new ConfigResource(ConfigResource.Type.TOPIC, _))
            .asJava

          F.delay(
            admin
              .describeConfigs(configResources)
              .all
              .get(timeout.toMillis, TimeUnit.MILLISECONDS)
          ).map(_.asScala.map { case (configResource, config) =>
            (
              configResource.name,
              config
                .entries()
                .asScala
                .map(configEntry => (configEntry.name, configEntry.value))
                .toMap
            )
          }.toMap)
        }

      override def close(): F[Unit] =
        CS.evalOn(blockingEC) {
          F.delay(admin.close())
        }

      override def close(timeout: Duration): F[Unit] =
        CS.evalOn(blockingEC) {
          F.delay(admin.close(java.time.Duration.ofNanos(timeout.toNanos)))
        }
    }
  // scalastyle:on method.length
}

// TODO we need to move this to commons. Shouldn't be exposed here
