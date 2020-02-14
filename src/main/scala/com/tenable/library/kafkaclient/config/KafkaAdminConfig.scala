package com.tenable.library.kafkaclient.config

import java.util.Properties

import org.apache.kafka.clients.admin.AdminClientConfig

import scala.concurrent.duration._

case class KafkaAdminConfig(idleTimeout: FiniteDuration, connectionString: String) {
  def properties: Properties = {
    val props = new Properties
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, connectionString)
    props.put(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, idleTimeout.toMillis.toString)
    props
  }
}

object KafkaAdminConfig {
  def default(connString: String): KafkaAdminConfig = new KafkaAdminConfig(10.seconds, connString)
}
