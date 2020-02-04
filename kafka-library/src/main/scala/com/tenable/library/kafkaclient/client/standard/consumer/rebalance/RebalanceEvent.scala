package com.tenable.library.kafkaclient.client.standard.consumer.rebalance

import java.util

import cats.syntax.applicativeError._
import cats.effect.ConcurrentEffect
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import cats.effect.concurrent.MVar
import com.github.ghik.silencer.silent

case class PartitionsRevoked(partitions: Set[TopicPartition])

class RebalanceListener[F[_]: ConcurrentEffect](
    consumerName: String,
    partitionsRevokedMVar: MVar[F, PartitionsRevoked]
) extends ConsumerRebalanceListener {
  private val logger: Logger =
    LoggerFactory.getLogger(s"kafka-rebalance-$consumerName".replace('.', '-'))

  private def runSync[A](fa: F[A]): Either[Throwable, A] =
    ConcurrentEffect[F]
      .toIO(fa.attempt)
      .unsafeRunSync()

  @silent
  override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
    logger.warn(
      s"Rebalance detected partitions revoked: ${partitions.asScala.toSet}, notifying consumer"
    )

    runSync(partitionsRevokedMVar.put(PartitionsRevoked(partitions.asScala.toSet))) match {
      case Left(_) =>
        logger.warn(
          s"Rebalance detected partitions revoked: ${partitions.asScala.toSet}, unable to notify customer"
        )
      case Right(_) =>
        logger.info(
          s"Rebalance detected partitions revoked: ${partitions.asScala.toSet}, customer notified"
        )
    }
  }

  @silent
  override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
    logger.info(s"Partitions assigned after revocation: ${partitions.asScala.toSet}")
    runSync(partitionsRevokedMVar.take) match {
      case Left(_) =>
        logger.warn(
          s"Partitions assigned after revocation: ${partitions.asScala.toSet}, unable to notify customer"
        )
      case Right(_) =>
        logger.info(
          s"Partitions assigned after revocation: ${partitions.asScala.toSet}, customer notified"
        )
    }
  }
}
