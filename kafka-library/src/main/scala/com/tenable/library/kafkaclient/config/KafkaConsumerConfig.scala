package com.tenable.library.kafkaclient.config

import java.util.Properties

import com.github.ghik.silencer.silent
import org.apache.kafka.clients.consumer.ConsumerConfig

import scala.concurrent.duration.{DurationLong, FiniteDuration}
import scala.collection.JavaConverters._

case class KafkaConsumerConfig(
    connectionString: String,
    topics: Set[String],
    groupId: String,
    maxPollInterval: FiniteDuration,
    maybeFakePollInterval: Option[FiniteDuration] = None,
    maybeMaxPollRecords: Option[Int] = None,
    customClientId: Option[String] = None,
    autoCommit: Boolean = false,
    autoOffsetReset: String = "earliest",
    maybeSessionTimeout: Option[FiniteDuration] = None,
    additionalConfig: Map[String, String] = Map.empty[String, String]
) {

  lazy val clientId: String = customClientId.getOrElse(s"$groupId-consumer")

  val fakePollInterval: FiniteDuration = maybeFakePollInterval.getOrElse {
    if (maxPollInterval < 2.seconds) maxPollInterval
    else if (maxPollInterval < 5.seconds) maxPollInterval - 2.seconds
    else 5.seconds
  }

  @silent
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
    props.putAll(additionalConfig.asJava)
    props
  }

}
