package com.tenable.library.kafkaclient.client.standard.consumer

import cats.Monad
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common._
import java.util.{List => JList}

import com.github.ghik.silencer.silent

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

@silent
object KafkaUnitTestUtils {
  class KafkaConsumerIORecording[F[_]: Monad] extends NoOpKafkaConsumerIO[F] {
    private val F                 = Monad[F]
    override val clientId: String = "unit-client"

    sealed trait RecordedAction
    case class PauseTPs(details: Map[TopicPartition, PausedTemporarily]) extends RecordedAction
    case class ResumeTPs(tps: Set[TopicPartition])                       extends RecordedAction
    case class Commit(offsets: Map[TopicPartition, Long])                extends RecordedAction
    case class CommitAsync(offsets: Map[TopicPartition, Long])           extends RecordedAction
    case class SeekWithError(offsets: Map[TopicPartition, Long], error: String)
        extends RecordedAction

    val recordedActions: ArrayBuffer[RecordedAction] = ArrayBuffer()

    override def pause(details: Map[TopicPartition, PausedTemporarily]): F[Unit] =
      F.pure(recordedActions.+=(PauseTPs(details)))

    override def resume(topicPartitions: Set[TopicPartition]): F[Unit] =
      F.pure(recordedActions.+=(ResumeTPs(topicPartitions)))

    override def commitSync(offsets: Map[TopicPartition, Long]): F[Unit] =
      F.pure(recordedActions.+=(Commit(offsets)))

    override def commitAsync(offsets: Map[TopicPartition, Long]): F[Unit] =
      F.pure(recordedActions.+=(CommitAsync(offsets)))

    override def seekWithError(
        offsets: Map[TopicPartition, Long],
        error: Option[String]
    ): F[Unit] =
      F.pure(recordedActions.+=(SeekWithError(offsets, error.getOrElse(""))))
  }

  def buildRecords(
      topic: String,
      partition: Int,
      kvs: String*
  ): Map[TopicPartition, JList[ConsumerRecord[String, String]]] = {
    val tp = new TopicPartition(topic, partition)
    val records = kvs.toList.zipWithIndex.map {
      case (kv, idx) => new ConsumerRecord(topic, partition, idx.toLong, kv, kv)
    }.asJava
    Map(tp -> records)
  }
}
