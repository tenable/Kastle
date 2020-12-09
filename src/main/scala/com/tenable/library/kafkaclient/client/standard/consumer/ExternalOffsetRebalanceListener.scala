package com.tenable.library.kafkaclient.client.standard.consumer

import cats.effect.implicits._
import cats.syntax.functor._
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import java.{util => ju}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer.KafkaConsumer
import com.github.ghik.silencer.silent
import scala.collection.JavaConverters._
import cats.effect.ConcurrentEffect

@silent
class ExternalOffsetRebalanceListener[F[_]: ConcurrentEffect](
    consumer: KafkaConsumer[_, _],
    findOffsets: Set[TopicPartition] => F[Map[TopicPartition, Option[Long]]]
) extends ConsumerRebalanceListener {
  def onPartitionsRevoked(partitions: ju.Collection[TopicPartition]): Unit = {
    //Do nothing
  }

  def onPartitionsAssigned(partitions: ju.Collection[TopicPartition]): Unit = {
    val partitionsSet = partitions.asScala.toSet
    findOffsets(partitionsSet).map { offsetMap =>
      offsetMap.toList.foreach { case (tp, offset) =>
        consumer.seek(tp, offset.map(_ + 1).getOrElse(0L))
      }
    }.toIO.unsafeRunSync() //Dealing with sync java API's here :)
  }

}

object ExternalOffsetRebalanceListener {
  def apply[F[_]: ConcurrentEffect](
      findOffsets: Set[TopicPartition] => F[Map[TopicPartition, Option[Long]]]
  )(c: KafkaConsumer[_, _]): ExternalOffsetRebalanceListener[F] = {
    new ExternalOffsetRebalanceListener[F](c, findOffsets)
  }
}
