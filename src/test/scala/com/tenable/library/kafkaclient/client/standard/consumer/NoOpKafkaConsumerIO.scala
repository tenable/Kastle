package com.tenable.library.kafkaclient.client.standard.consumer

import java.{util => ju}

import cats.Monad
import com.tenable.library.kafkaclient.client.standard.KafkaConsumerIO
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import cats.syntax.functor._

import scala.concurrent.duration.Duration

class NoOpKafkaConsumerIO[F[_]: Monad] extends KafkaConsumerIO[F, Nothing, Nothing] {
  private val F                 = Monad[F]
  override val clientId: String = "unit-client"

  override def poll[R](timeout: Duration)(f: ConsumerRecords[Nothing, Nothing] => R): F[R] =
    rawPoll(timeout).map(f)
  override def rawPoll(timeout: Duration): F[ConsumerRecords[Nothing, Nothing]] =
    F.pure(emptyConsumerRecords)

  override def close(): F[Unit]                                                      = F.unit
  override def close(timeout: Duration): F[Unit]                                     = F.unit
  override def pause(topic: String): F[Unit]                                         = F.unit
  override def pause(details: Map[TopicPartition, PausedTemporarily]): F[Unit]       = F.unit
  override def paused(): F[Set[TopicPartition]]                                      = set
  override def resume(topic: String): F[Unit]                                        = F.unit
  override def resume(topicPartitions: Set[TopicPartition]): F[Unit]                 = F.unit
  override def commitSync(offsets: Map[TopicPartition, Long]): F[Unit]               = F.unit
  override def commitAsync(offsets: Map[TopicPartition, Long]): F[Unit]              = F.unit
  override def listTopics(): F[Map[String, List[PartitionInfo]]]                     = map
  override def listTopics(duration: Duration): F[Map[String, List[PartitionInfo]]]   = map
  override def position(topicPartition: TopicPartition): F[Long]                     = long
  override def position(topicPartition: TopicPartition, duration: Duration): F[Long] = long
  override def assignment(): F[Set[TopicPartition]]                                  = set
  override def seek(topicPartition: TopicPartition, offset: Long): F[Unit]           = F.unit
  override def seekToEnd(topicPartitions: List[TopicPartition]): F[Unit]             = F.unit
  override def seekToBeginning(topicPartitions: List[TopicPartition]): F[Unit]       = F.unit
  override def restartOnError(error: Throwable): F[Unit]                             = F.unit
  override def committed(topicPartition: TopicPartition): F[OffsetAndMetadata] =
    emptyOffsetAndMetadata
  override def committed(topicPartition: TopicPartition, duration: Duration): F[OffsetAndMetadata] =
    emptyOffsetAndMetadata
  override def seekWithError(
      offsets: Map[TopicPartition, Long],
      error: Option[String]
  ): F[Unit] = F.unit
  override def pollForever
      : KafkaRunLoop.Builder[KafkaRunLoop.CreatedEmpty, F, Nothing, Nothing, Nothing, Nothing] =
    throw new NotImplementedError("Cant poll forever on a no op consumer")

  private def set[T]: F[Set[T]]       = F.pure(Set.empty[T])
  private def map[K, V]: F[Map[K, V]] = F.pure(Map.empty[K, V])
  private def emptyConsumerRecords[K, V]: ConsumerRecords[K, V] =
    new ConsumerRecords(new ju.HashMap[TopicPartition, ju.List[ConsumerRecord[K, V]]]())

  private val long                   = F.pure(Long.MinValue)
  private val emptyOffsetAndMetadata = long.map(new OffsetAndMetadata(_))
}
