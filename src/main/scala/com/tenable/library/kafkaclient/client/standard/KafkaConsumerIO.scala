package com.tenable.library.kafkaclient.client.standard

import java.util

import cats.effect._
import cats.syntax.apply._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.effect.concurrent.{ MVar, MVar2 }
import com.tenable.library.kafkaclient.client.standard.consumer.{
  ConsumerStateHandler,
  KafkaRunLoop,
  PausedTemporarily,
  State
}
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.{ PartitionInfo, TopicPartition }
import org.apache.kafka.common.serialization.Deserializer
import org.slf4j.{ Logger, LoggerFactory }

import scala.jdk.CollectionConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import com.tenable.library.kafkaclient.utils.ExecutionContexts
import com.tenable.library.kafkaclient.utils.Converters.JavaDurationOps
import com.tenable.library.kafkaclient.config.KafkaConsumerConfig
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener

import scala.annotation.nowarn

trait KafkaConsumerIO[F[_], K, V] {
  val clientId: String

  def poll[R](timeout: Duration)(f: ConsumerRecords[K, V] => R): F[R]
  def rawPoll(timeout: Duration): F[ConsumerRecords[K, V]]
  def pollForever: KafkaRunLoop.Builder[KafkaRunLoop.CreatedEmpty, F, K, V, Nothing, Nothing]
  def close(): F[Unit]
  def close(timeout: Duration): F[Unit]
  def pause(topic: String): F[Unit]
  def pause(details: Map[TopicPartition, PausedTemporarily]): F[Unit]
  def paused(): F[Set[TopicPartition]]
  def resume(topic: String): F[Unit]
  def resume(topicPartitions: Set[TopicPartition]): F[Unit]
  def commitSync(offsets: Map[TopicPartition, Long]): F[Unit]
  def commitAsync(offsets: Map[TopicPartition, Long]): F[Unit]
  def listTopics(): F[Map[String, List[PartitionInfo]]]
  def listTopics(duration: Duration): F[Map[String, List[PartitionInfo]]]
  def position(topicPartition: TopicPartition): F[Long]
  def position(topicPartition: TopicPartition, duration: Duration): F[Long]
  def committed(topicPartition: TopicPartition): F[OffsetAndMetadata]
  def committed(topicPartition: TopicPartition, duration: Duration): F[OffsetAndMetadata]
  def assignment(): F[Set[TopicPartition]]
  def seek(topicPartition: TopicPartition, offset: Long): F[Unit]
  def seekToEnd(topicPartitions: List[TopicPartition]): F[Unit]
  def seekToBeginning(topicPartitions: List[TopicPartition]): F[Unit]
  def seekWithError(offsets: Map[TopicPartition, Long], error: Option[String]): F[Unit]
  def restartOnError(error: Throwable): F[Unit]
}

object KafkaConsumerIO {
  sealed trait BuilderState
  sealed trait CreatedEmpty          extends BuilderState
  sealed trait WithKeyDeserializer   extends BuilderState
  sealed trait WithValueDeserializer extends BuilderState

  type Initialized =
    CreatedEmpty with WithKeyDeserializer with WithValueDeserializer

  class Builder[
    T <: BuilderState,
    F[_]: ConcurrentEffect: ContextShift: Timer,
    K,
    V
  ] private[KafkaConsumerIO] (
    config: KafkaConsumerConfig,
    keyDeserializer: Option[Deserializer[K]],
    valueDeserializer: Option[Deserializer[V]],
    blockingEC: Option[Resource[F, ExecutionContext]],
    rebalanceListener: KafkaConsumer[K, V] => ConsumerRebalanceListener = (_: KafkaConsumer[K, V]) =>
      new NoOpConsumerRebalanceListener
  ) {
    type OngoingBuilder[TT <: BuilderState] = Builder[TT, F, K, V]

    def withKeyDeserializer(
      keyDeserializer: Deserializer[K]
    ): OngoingBuilder[T with WithKeyDeserializer] =
      new Builder[T with WithKeyDeserializer, F, K, V](
        config,
        Some(keyDeserializer),
        valueDeserializer,
        blockingEC,
        rebalanceListener
      )

    def withValueDeserializer(
      valueDeserializer: Deserializer[V]
    ): OngoingBuilder[T with WithValueDeserializer] =
      new Builder[T with WithValueDeserializer, F, K, V](
        config,
        keyDeserializer,
        Some(valueDeserializer),
        blockingEC,
        rebalanceListener
      )

    def withBlockingEC(blockingEC: Resource[F, ExecutionContext]): OngoingBuilder[T] =
      new Builder[T, F, K, V](
        config,
        keyDeserializer,
        valueDeserializer,
        Some(blockingEC),
        rebalanceListener
      )

    def rebalanceListener(l: KafkaConsumer[K, V] => ConsumerRebalanceListener): OngoingBuilder[T] =
      new Builder[T, F, K, V](
        config,
        keyDeserializer,
        valueDeserializer,
        blockingEC,
        l
      )

    def resource(implicit ev: Initialized =:= T): Resource[F, KafkaConsumerIO[F, K, V]] = {
      val _ = ev //shutup compiler
      KafkaConsumerIO.resource(
        config,
        keyDeserializer.get,
        valueDeserializer.get,
        blockingEC,
        rebalanceListener
      )
    }
  }

  def builder[F[_]: ConcurrentEffect: ContextShift: Timer, K, V](
    config: KafkaConsumerConfig
  ): Builder[CreatedEmpty, F, K, V] =
    new Builder[CreatedEmpty, F, K, V](config, None, None, None)

  private def resource[F[_]: ConcurrentEffect: ContextShift: Timer, K, V](
    config: KafkaConsumerConfig,
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V],
    optionalBlockingEC: Option[Resource[F, ExecutionContext]],
    rebalanceListener: KafkaConsumer[K, V] => ConsumerRebalanceListener
  ): Resource[F, KafkaConsumerIO[F, K, V]] =
    for {
      ec <- optionalBlockingEC.getOrElse(ExecutionContexts.io(s"kafka-consumer"))
      consumer <- Resource.make(
        create[F, K, V](
          config,
          keyDeserializer,
          valueDeserializer,
          ec,
          rebalanceListener
        )
      )(
        _.close()
      )
    } yield consumer

  private def create[F[_]: ConcurrentEffect: ContextShift: Timer, K, V](
    config: KafkaConsumerConfig,
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V],
    blockingEC: ExecutionContext,
    rebalanceListener: KafkaConsumer[K, V] => ConsumerRebalanceListener
  ): F[KafkaConsumerIO[F, K, V]] = {
    implicit val logger: Logger =
      LoggerFactory.getLogger(s"kafka-io-${config.clientId}".replace('.', '-'))

    val consumerBuilder: () => KafkaConsumer[K, V] = { () =>
      val c =
        new KafkaConsumer[K, V](
          config.properties,
          keyDeserializer,
          valueDeserializer
        )
      val subscribedTopics = config.topics.asJava
      c.subscribe(subscribedTopics, rebalanceListener(c))
      c
    }

    val consumerStateHandler: MVar2[F, State[F, K, V]] => ConsumerStateHandler[F, K, V] =
      new ConsumerStateHandler[F, K, V](
        config.clientId,
        config.fakePollInterval,
        consumerBuilder,
        _,
        blockingEC
      )

    MVar[F]
      .empty[consumer.State[F, K, V]]
      .map(consumerStateHandler)
      .flatTap(_.unsafeStart(Set.empty))
      .map(apply[F, K, V])
  }

  // scalastyle:off method.length
  private def apply[F[_]: ConcurrentEffect: Timer, K, V](
    stateHandler: ConsumerStateHandler[F, K, V]
  )(implicit logger: Logger): KafkaConsumerIO[F, K, V] = new KafkaConsumerIO[F, K, V] {
    private val F = ConcurrentEffect[F]

    override val clientId: String = stateHandler.clientId

    override def pollForever: KafkaRunLoop.Builder[KafkaRunLoop.CreatedEmpty, F, K, V, Nothing, Nothing] =
      KafkaRunLoop.builder(this)

    override def rawPoll(timeout: Duration): F[ConsumerRecords[K, V]] =
      stateHandler.refreshTemporarilyPaused() *>
        stateHandler.withConsumer("poll") { state =>
          if (state.isClosed) {
            F.pure((state, emptyConsumerRecords))
          } else {
            F.delay {
              logger.debug(s"Polling")
              (state, state.consumer.poll(timeout.asJavaDuration))
            }
          }
        }

    override def poll[R](timeout: Duration)(f: ConsumerRecords[K, V] => R): F[R] =
      rawPoll(timeout).map(f)

    override def close(): F[Unit] =
      stateHandler.closeConsumer(timeout = None)

    override def close(timeout: Duration): F[Unit] =
      stateHandler.closeConsumer(timeout = Some(timeout))

    override def pause(topic: String): F[Unit] =
      stateHandler.withConsumer("pause", Some(s"Pausing topic $topic")) { state =>
        F.delay {
          val tps = state.consumer.assignment().asScala.filter(_.topic() == topic)
          state.consumer.pause(tps.asJava)
          (state.copy(pausedT = state.pausedT + topic), ())
        }
      }

    override def pause(details: Map[TopicPartition, PausedTemporarily]): F[Unit] =
      stateHandler.withConsumer(
        "batch-pause",
        Some(s"Pausing ${details.map { case (k, v) => s"$k -> $v" }}")
      ) { state =>
        F.delay {
          state.consumer.pause(details.keySet.asJava)
          (state.copy(pausedTP = state.pausedTP ++ details), ())
        }
      }

    override def paused(): F[Set[TopicPartition]] =
      stateHandler.withConsumer("paused", Some(s"Listing paused topics")) { state =>
        F.delay {
          (state, state.consumer.paused().asScala.toSet)
        }
      }

    override def resume(topic: String): F[Unit] =
      stateHandler.withConsumer("resume", Some(s"Resuming topic $topic")) { state =>
        F.delay {
          val tps = state.consumer.assignment().asScala.filter(_.topic() == topic)
          state.consumer.resume(tps.asJava)
          (state.copy(pausedT = state.pausedT - topic), ())
        }
      }

    override def resume(topicPartitions: Set[TopicPartition]): F[Unit] =
      stateHandler.withConsumer("batch-resume", Some(s"Resuming $topicPartitions")) { state =>
        F.delay {
          val tps = state.consumer.assignment().asScala.filter(topicPartitions)
          state.consumer.resume(tps.asJava)
          (
            state.copy(pausedTP = state.pausedTP.filterNot { case (k, _) =>
              topicPartitions.contains(k)
            }),
            ()
          )
        }
      }

    override def commitSync(offsets: Map[TopicPartition, Long]): F[Unit] =
      stateHandler.withConsumer("batch-commit") { state =>
        F.delay {
          state
            .consumer
            .commitSync(
              offsets.view.mapValues(o => new OffsetAndMetadata(o)).toMap.asJava
            )
          (state, ())
        }
      }

    // scalastyle:off null
    override def commitAsync(offsets: Map[TopicPartition, Long]): F[Unit] =
      stateHandler.withConsumer("batch-commit-async") { state =>
        F.delay {
          state
            .consumer
            .commitAsync(
              offsets.view.mapValues(o => new OffsetAndMetadata(o)).toMap.asJava,
              null
            )
          (state, ())
        }
      }
    // scalastyle:on null

    override def listTopics(): F[Map[String, List[PartitionInfo]]] =
      stateHandler.withConsumer("list-topics", Some(s"Listing all topics")) { state =>
        F.delay {
          (state, state.consumer.listTopics().asScala.view.mapValues(_.asScala.toList).toMap)
        }
      }

    override def listTopics(duration: Duration): F[Map[String, List[PartitionInfo]]] =
      stateHandler
        .withConsumer("list-topics-by-duration", Some(s"Listing all topics for $duration")) { state =>
          F.delay {
            (
              state,
              state
                .consumer
                .listTopics(duration.asJavaDuration)
                .asScala
                .view
                .mapValues(_.asScala.toList)
                .toMap
            )
          }
        }

    override def position(topicPartition: TopicPartition): F[Long] =
      stateHandler.withConsumer("position", Some(s"Getting position for $topicPartition")) { state =>
        F.delay {
          (state, state.consumer.position(topicPartition))
        }
      }

    override def position(topicPartition: TopicPartition, duration: Duration): F[Long] =
      stateHandler.withConsumer(
        "position-by-duration",
        Some(s"Getting position for $topicPartition and $duration")
      ) { state =>
        F.delay {
          (state, state.consumer.position(topicPartition, duration.asJavaDuration))
        }
      }

    @nowarn // FIXME: committed is deprecated
    override def committed(topicPartition: TopicPartition): F[OffsetAndMetadata] =
      stateHandler
        .withConsumer("committed", Some(s"Getting committed offsets for $topicPartition")) { state =>
          F.delay {
            (state, state.consumer.committed(topicPartition))
          }
        }

    @nowarn // FIXME: committed is deprecated
    override def committed(
      topicPartition: TopicPartition,
      duration: Duration
    ): F[OffsetAndMetadata] =
      stateHandler.withConsumer(
        "committed-by-duration",
        Some(s"Getting committed offsets for $topicPartition and $duration")
      ) { state =>
        F.delay {
          (state, state.consumer.committed(topicPartition, duration.asJavaDuration))
        }
      }

    override def assignment(): F[Set[TopicPartition]] =
      stateHandler.withConsumer("assignment", Some(s"Getting assignment")) { state =>
        F.delay {
          (state, state.consumer.assignment().asScala.toSet)
        }
      }

    override def seek(topicPartition: TopicPartition, offset: Long): F[Unit] =
      stateHandler.withConsumer("seek", Some(s"Seeking to $offset for $topicPartition")) { state =>
        F.delay {
          (state, state.consumer.seek(topicPartition, offset))
        }
      }

    override def seekToEnd(topicPartitions: List[TopicPartition]): F[Unit] =
      stateHandler.withConsumer("seek-to-end", Some(s"Seeking to end for $topicPartitions")) { state =>
        F.delay {
          (state, state.consumer.seekToEnd(topicPartitions.asJava))
        }
      }

    override def seekToBeginning(topicPartitions: List[TopicPartition]): F[Unit] =
      stateHandler
        .withConsumer("seek-to-beginning", Some(s"Seeking to beginning for $topicPartitions")) { state =>
          F.delay {
            (state, state.consumer.seekToBeginning(topicPartitions.asJava))
          }
        }

    override def seekWithError(
      offsets: Map[TopicPartition, Long],
      error: Option[String]
    ): F[Unit] =
      stateHandler.withConsumer("batch-seek-with-error") { state =>
        F.delay {
          val msg = s"Seeking (${offsets.map { case (k, v) => s"$k -> $v" }}"
          error.fold(logger.info(msg))(err => logger.error(s"$msg, due to error $err"))
          offsets.foreach({ case (partition, offset) => state.consumer.seek(partition, offset) })
          (state, ())
        }
      }

    def restartOnError(error: Throwable): F[Unit] =
      stateHandler.restartOnError(error)

    private def emptyConsumerRecords: ConsumerRecords[K, V] =
      new ConsumerRecords(new util.HashMap[TopicPartition, util.List[ConsumerRecord[K, V]]]())
  }
  // scalastyle:on method.length
}
