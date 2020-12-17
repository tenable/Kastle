package com.tenable.library.kafkaclient.config

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig

import scala.concurrent.duration.{DurationLong, FiniteDuration}

case class KafkaConsumerConfig(
    connectionString: String,
    topics: Set[String],
    groupId: String,
    maxPollInterval: FiniteDuration,
    keepAliveInterval: Option[FiniteDuration] = None,
    maybeMaxPollRecords: Option[Int] = None,
    customClientId: Option[String] = None,
    autoCommit: Boolean = false,
    autoOffsetReset: String = "earliest",
    maybeSessionTimeout: Option[FiniteDuration] = None,
    additionalConfig: Map[String, String] = Map.empty[String, String]
) {

  lazy val clientId: String = customClientId.getOrElse(s"$groupId-consumer")

  val fakePollInterval: FiniteDuration = keepAliveInterval.getOrElse(0.millis)

  def properties[K, V]: Properties = {
    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connectionString)
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId)
    props.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollInterval.toMillis.toString)
    maybeMaxPollRecords.foreach { max =>
      props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, max.toString)
    }
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit.toString)
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    maybeSessionTimeout.foreach { timeout =>
      props.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, timeout.toMillis.toString)
    }
    additionalConfig.foreach { case (k, v) => props.setProperty(k, v) }
    props
  }

}
