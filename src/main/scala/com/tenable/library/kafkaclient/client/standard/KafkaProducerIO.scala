package com.tenable.library.kafkaclient.client.standard

import cats.instances.list._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.traverse._
import com.tenable.library.kafkaclient.utils.ExecutionContexts
import com.tenable.library.kafkaclient.config.KafkaProducerConfig
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.Serializer
import com.tenable.library.kafkaclient.utils.Converters.JavaFutureOps

import scala.concurrent.ExecutionContext
import cats.effect.ContextShift
import cats.effect.Resource
import org.apache.kafka.common.PartitionInfo

import scala.concurrent.duration.Duration
import scala.collection.JavaConverters._
import cats.effect.Async
import com.github.ghik.silencer.silent

trait KafkaProducerIO[F[_], K, V] { self =>

  def send(outputTopic: String, key: K, value: V): F[RecordMetadata] =
    send(outputTopic, key, value, identity)
  def send(
      outputTopic: String,
      key: K,
      value: V,
      decorate: ProducerRecord[K, V] => ProducerRecord[K, V]
  ): F[RecordMetadata]

  def sendMany(outputTopic: String, keyValues: List[(K, V)]): F[List[RecordMetadata]] =
    sendMany(outputTopic, keyValues, identity)
  def sendMany(
      outputTopic: String,
      keyValues: List[(K, V)],
      decorate: ProducerRecord[K, V] => ProducerRecord[K, V]
  ): F[List[RecordMetadata]]

  /**
    * When sending an event using the asynchronous Kafka client, there are two stages of `done`:
    * 1) When the event has been pushed onto the client's buffer. This is the point at which the Java Future returned by
    *    the `KafkaProducer#send` method completes
    * 2) When the event has been flushed from the local buffer out to Kafka itself. This is when the optional callback
    *    parameter of `send` is invoked
    *
    * The following two methods are asynchronous, but complete at different stages - `bufferedSend` completes as soon
    * as the event is written to the buffer, and `send` ensures that the event reaches Kafka itself. You probably want to
    * use `send` in most cases.
    *
    * Use producer settings to tweak the performance and the behaviour implications:
    *
    * `linger.ms` (default 0) to control the maximum time a record can stay in the buffer
    * `batch.size` (default 16384) to control the maximum size per produced batch
    *
    * There are other settings that affect this behaviour, but those two are the most important.
    */
  def sendAndForget(outputTopic: String, key: K, value: V): F[RecordMetadata] =
    sendAndForget(outputTopic, key, value, identity)
  def sendAndForget(
      outputTopic: String,
      key: K,
      value: V,
      decorate: ProducerRecord[K, V] => ProducerRecord[K, V]
  ): F[RecordMetadata]

  def sendAndForgetMany(outputTopic: String, keyValues: List[(K, V)]): F[List[RecordMetadata]] =
    sendAndForgetMany(outputTopic, keyValues, identity)
  def sendAndForgetMany(
      outputTopic: String,
      keyValues: List[(K, V)],
      decorate: ProducerRecord[K, V] => ProducerRecord[K, V]
  ): F[List[RecordMetadata]]

  def flush(): F[Unit]
  def close(): F[Unit]
  def close(timeout: Duration): F[Unit]
  def partitionsFor(topicName: String): F[List[PartitionInfo]]

  class ForTopic[T](topicName: String) {
    def unsafeSend(k: K, v: V): F[RecordMetadata] = self.sendAndForget(topicName, k, v)
    def unsafeSend(
        k: K,
        v: V,
        decorate: ProducerRecord[K, V] => ProducerRecord[K, V]
    ): F[RecordMetadata]                    = self.sendAndForget(topicName, k, v, decorate)
    def send(k: K, v: V): F[RecordMetadata] = self.send(topicName, k, v)
    def send(
        k: K,
        v: V,
        decorate: ProducerRecord[K, V] => ProducerRecord[K, V]
    ): F[RecordMetadata] = self.send(topicName, k, v, decorate)
  }

  def forTopic[T](topicName: String): ForTopic[T] = new ForTopic[T](topicName)
}

object KafkaProducerIO {
  sealed trait BuilderState
  sealed trait CreatedEmpty          extends BuilderState
  sealed trait WithKeyDeserializer   extends BuilderState
  sealed trait WithValueDeserializer extends BuilderState
  sealed trait WithExecutionContext  extends BuilderState

  type Initialized =
    CreatedEmpty with WithKeyDeserializer with WithValueDeserializer

  class Builder[T <: BuilderState, F[_]: Async: ContextShift, K, V] private[KafkaProducerIO] (
      config: KafkaProducerConfig,
      keyDeserializer: Option[Serializer[K]],
      valueDeserializer: Option[Serializer[V]],
      blockingEC: Option[Resource[F, ExecutionContext]]
  ) {
    type OngoingBuilder[TT <: BuilderState] = Builder[TT, F, K, V]

    def withKeyDeserializer(
        keyDeserializer: Serializer[K]
    ): OngoingBuilder[T with WithKeyDeserializer] =
      new Builder[T with WithKeyDeserializer, F, K, V](
        config,
        Some(keyDeserializer),
        valueDeserializer,
        blockingEC
      )

    def withValueDeserializer(
        valueDeserializer: Serializer[V]
    ): OngoingBuilder[T with WithValueDeserializer] =
      new Builder[T with WithValueDeserializer, F, K, V](
        config,
        keyDeserializer,
        Some(valueDeserializer),
        blockingEC
      )

    def withBlockingEC(blockingEC: Resource[F, ExecutionContext]): OngoingBuilder[T] =
      new Builder[T, F, K, V](
        config,
        keyDeserializer,
        valueDeserializer,
        Some(blockingEC)
      )

    def resource(implicit ev: Initialized =:= T): Resource[F, KafkaProducerIO[F, K, V]] = {
      val _ = ev //shutup compiler
      KafkaProducerIO.resource(
        config,
        keyDeserializer.get,
        valueDeserializer.get,
        blockingEC
      )
    }
  }

  def builder[F[_]: Async: ContextShift, K, V](
      config: KafkaProducerConfig
  ): Builder[CreatedEmpty, F, K, V] =
    new Builder[CreatedEmpty, F, K, V](config, None, None, None)

  private def resource[F[_]: Async: ContextShift, K, V](
      config: KafkaProducerConfig,
      keySerializer: Serializer[K],
      valueSerializer: Serializer[V],
      optionalBlockingEC: Option[Resource[F, ExecutionContext]]
  ): Resource[F, KafkaProducerIO[F, K, V]] =
    for {
      blockingEC <- optionalBlockingEC.getOrElse(ExecutionContexts.io("kafka-producer-io"))
      producer <- Resource.make(
                   create[F, K, V](
                     config,
                     keySerializer,
                     valueSerializer,
                     blockingEC
                   )
                 )(_.close())
    } yield producer

  private def create[F[_]: Async: ContextShift, K, V](
      config: KafkaProducerConfig,
      keySerializer: Serializer[K],
      valueSerializer: Serializer[V],
      blockingEC: ExecutionContext
  ): F[KafkaProducerIO[F, K, V]] =
    Async[F].delay {
      apply(
        new KafkaProducer[K, V](
          config.properties,
          keySerializer,
          valueSerializer
        ),
        blockingEC
      )
    }

  // scalastyle:off method.length
  private def apply[F[_]: Async: ContextShift, K, V](
      producer: Producer[K, V],
      blockingEC: ExecutionContext
  ): KafkaProducerIO[F, K, V] = new KafkaProducerIO[F, K, V] { self =>
    private val F  = Async[F]
    private val CS = ContextShift[F]

    override def send(
        outputTopic: String,
        key: K,
        value: V,
        decorate: ProducerRecord[K, V] => ProducerRecord[K, V]
    ): F[RecordMetadata] =
      CS.evalOn(blockingEC) {
        sendConfirmed(outputTopic, key, value, decorate)
      }

    override def sendMany(
        outputTopic: String,
        keyValues: List[(K, V)],
        decorate: ProducerRecord[K, V] => ProducerRecord[K, V]
    ): F[List[RecordMetadata]] =
      CS.evalOn(blockingEC) {
        keyValues.traverse {
          case (key, value) => sendConfirmed(outputTopic, key, value, decorate)
        }
      }

    override def sendAndForget(
        outputTopic: String,
        key: K,
        value: V,
        decorate: ProducerRecord[K, V] => ProducerRecord[K, V]
    ): F[RecordMetadata] =
      CS.evalOn(blockingEC) {
        sendUnconfirmed(outputTopic, key, value, decorate)
      }

    override def sendAndForgetMany(
        outputTopic: String,
        keyValues: List[(K, V)],
        decorate: ProducerRecord[K, V] => ProducerRecord[K, V]
    ): F[List[RecordMetadata]] =
      CS.evalOn(blockingEC) {
        keyValues.traverse {
          case (key, value) => sendUnconfirmed(outputTopic, key, value, decorate)
        }
      }

    override def flush(): F[Unit] =
      F.delay {
        producer.flush()
      }

    override def close(): F[Unit] =
      F.delay {
        producer.close()
      }

    override def close(timeout: Duration): F[Unit] =
      F.delay {
        producer.close(java.time.Duration.ofNanos(timeout.toNanos))
      }

    @silent
    override def partitionsFor(topicName: String): F[List[PartitionInfo]] =
      CS.evalOn(blockingEC) {
        F.delay(producer.partitionsFor(topicName).asScala.toList)
      }

    private def producerRecord(outputTopic: String, key: K, value: V) =
      new ProducerRecord[K, V](outputTopic, key, value)

    private def sendUnconfirmed(
        outputTopic: String,
        key: K,
        value: V,
        decorate: ProducerRecord[K, V] => ProducerRecord[K, V]
    ): F[RecordMetadata] =
      for {
        futureRecord <- F.catchNonFatal(
                         producer.send(decorate(producerRecord(outputTopic, key, value)))
                       )
        record <- futureRecord.liftJF
      } yield record

    private def sendConfirmed(
        outputTopic: String,
        key: K,
        value: V,
        decorate: ProducerRecord[K, V] => ProducerRecord[K, V]
    ): F[RecordMetadata] =
      F.async[RecordMetadata] { cb =>
        producer.send(
          decorate(producerRecord(outputTopic, key, value)),
          (metadata: RecordMetadata, exception: Exception) => {
            if (exception == null) {
              cb(Right(metadata))
            } else {
              cb(Left(exception))
            }
          }
        )
        ()
      }
  }
  // scalastyle:on method.length

}
