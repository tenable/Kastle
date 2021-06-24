package com.tenable.library.kafkaclient.client.standard.consumer

import cats.Show
import cats.effect._
import cats.syntax.applicativeError._
import cats.syntax.apply._
import cats.syntax.flatMap._
import cats.syntax.functor._
import com.tenable.library.kafkaclient.client.standard.consumer.units.TPBatch
import com.tenable.library.kafkaclient.client.standard.consumer.KafkaProcessable.GAndOffsets
import com.tenable.library.kafkaclient.client.standard.consumer.actions.ProcessAction
import org.apache.kafka.common.KafkaException
import org.apache.kafka.clients.consumer.{ ConsumerRecord, ConsumerRecords }
import com.tenable.library.kafkaclient.client.standard.KafkaConsumerIO
import org.slf4j.{ Logger, LoggerFactory }

import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.util.Try
import cats.effect.Temporal

class KafkaRunLoop[F[_]: ConcurrentEffect, K, V] private (
  consumer: KafkaConsumerIO[F, K, V]
)(implicit T: Temporal[F]) { self =>
  private val F: ConcurrentEffect[F] = ConcurrentEffect[F]
  private val logger: Logger =
    LoggerFactory.getLogger(s"kafka-run-loop-${consumer.clientId}".replace('.', '-'))

  def pollForever[G[_, _]: KafkaProcessable, A: EventActionable](
    pollTimeout: FiniteDuration,
    process: G[K, V] => F[A]
  ): F[CancelToken[F]] = {
    val runG: GAndOffsets[G[K, V]] => F[BatchContext] = (gAndOffsets: GAndOffsets[G[K, V]]) => {
      val (g, topicOffsets) = gAndOffsets

      for {
        a <- process(g)
        action = EventActionable[A].asProcessAction(a)
        ctx <- ProcessAction.interpret(consumer, action, topicOffsets)
      } yield ctx
    }

    val pollingF = consumer
      .poll(pollTimeout)(KafkaProcessable.build[K, V, G])
      .flatMap {
        _.fold(T.sleep(500.millis)) { case (rejectOffsets, batchG) =>
          KafkaProcessable.run[F, K, V, G](batchG, runG).recoverWith { case err: KafkaException =>
            F.pure(logger.warn("Exception when handling kafka processable:", err)) *>
              consumer.seekWithError(rejectOffsets, None)
          }
        }
      }
      .recoverWith { case t =>
        T.sleep(500.millis) *> consumer.restartOnError(t)
      }

    F.start(pollingF.foreverM[Unit]).map(_.cancel)
  }
}

object KafkaRunLoop {
  sealed trait BuilderState
  sealed trait CreatedEmpty      extends BuilderState
  sealed trait WithGranularity   extends BuilderState
  sealed trait WithActionHandler extends BuilderState

  type Initialized = WithActionHandler with WithGranularity with CreatedEmpty

  class Builder[
    T <: BuilderState,
    F[_]: ConcurrentEffect: Temporal,
    K,
    V,
    G[_, _],
    A
  ] private[KafkaRunLoop] (
    consumer: KafkaConsumerIO[F, K, V],
    granularity: Option[KafkaProcessable[G]],
    actionHandler: Option[EventActionable[A]]
  ) {
    type OngoingBuilder[TT <: BuilderState, GG[_, _], AA] = Builder[TT, F, K, V, GG, AA]

    /**
      * Define how the source batch is going to be consumed. E.g: Event by event or Topic partition batch, etc...
      */
    def consuming[G1[_, _]](
      implicit KP: KafkaProcessable[G1]
    ): OngoingBuilder[T with WithGranularity, G1, A] =
      new Builder[T with WithGranularity, F, K, V, G1, A](
        consumer,
        Some(KafkaProcessable[G1]),
        actionHandler
      )

    def consumingSingleEvents: OngoingBuilder[T with WithGranularity, ConsumerRecord, A] =
      consuming[ConsumerRecord](KafkaProcessable.kafkaProcessableSingle)

    import TPBatch.TPRecords
    def consumingTopicPartitionBatch: OngoingBuilder[T with WithGranularity, TPRecords, A] =
      consuming[TPRecords](KafkaProcessable.kafkaProcessableTPRecords)

    def consumingFullBatch: OngoingBuilder[T with WithGranularity, ConsumerRecords, A] =
      consuming[ConsumerRecords](KafkaProcessable.kafkaProcessableFullBatch)

    /**
      * Define the data (A) to be returned by the process function (G[K, V] => F[A])
      */
    def expecting[A1](implicit EA: EventActionable[A1]): OngoingBuilder[T with WithActionHandler, G, A1] =
      new Builder[T with WithActionHandler, F, K, V, G, A1](
        consumer,
        granularity,
        Some(EventActionable[A1])
      )

    def expectingEither[E: Show]: OngoingBuilder[T with WithActionHandler, G, Either[E, Unit]] =
      expecting[Either[E, Unit]](EventActionable.deriveFromEither)

    def expectingTry(implicit S: Show[Throwable]): OngoingBuilder[T with WithActionHandler, G, Try[Unit]] =
      expecting[Try[Unit]](EventActionable.deriveFromTry)

    def expectingProcessAction: OngoingBuilder[T with WithActionHandler, G, ProcessAction] =
      expecting[ProcessAction](EventActionable.deriveFromProcessAction)

    /**
      * If you want to stop your cancelable processing function if a rebalance occurs
      */
    def withRebalanceDetector: OngoingBuilder[T, G, A] =
      new Builder[T, F, K, V, G, A](
        consumer,
        granularity,
        actionHandler
      )

    /**
      * Runs the consumer loop, which will poll forever until explicitly cancelled
      */
    def run(
      pollTimeout: FiniteDuration
    )(process: G[K, V] => F[A])(implicit ev: Initialized =:= T): F[CancelToken[F]] = {
      val _ = ev //shutup compiler
      new KafkaRunLoop[F, K, V](consumer)
        .pollForever(pollTimeout, process)(granularity.get, actionHandler.get)
    }
  }

  def builder[F[_]: ConcurrentEffect: Temporal, K, V](
    consumer: KafkaConsumerIO[F, K, V]
  ): Builder[CreatedEmpty, F, K, V, Nothing, Nothing] =
    new Builder[CreatedEmpty, F, K, V, Nothing, Nothing](consumer, None, None)
}
