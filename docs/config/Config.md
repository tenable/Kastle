# Config
This directory contains classes for kafka producer, consumer and topic configs.  Helpful builder methods are also 
available.

### KafkaConnectionString

```scala mdoc
import com.tenable.library.kafkaclient.config.KafkaConnectionString
import com.tenable.library.kafkaclient.config.builders.hocon.KafkaConnectionStringBuilder
import com.typesafe.config.ConfigFactory


val config = ConfigFactory.parseString(s"""brokers: ["localhost:6001"]""")

KafkaConnectionStringBuilder.buildFrom(config)
```

### KafkaConsumerConfig

```scala mdoc:reset
import com.tenable.library.kafkaclient.config.builders.hocon.KafkaConsumerConfigBuilder
import com.tenable.library.kafkaclient.config.KafkaConsumerConfig
import com.tenable.library.kafkaclient.naming.{ConsumerGroupName, TopicName}
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._


val ref =
  KafkaConsumerConfig(
    topics = Set(TopicName("prefix.pub.service.one.1"), TopicName("prefix.pub.service.two.1")),
    groupId = ConsumerGroupName("prefix.groupid.1"),
    maxPollInterval = 10.seconds,
    maybeFakePollInterval = Some(5.seconds),
    maybeMaxPollRecords = Some(1),
    customClientId = Some("custom-client-id"),
    autoCommit = true,
    autoOffsetReset = "latest",
    maybeSessionTimeout = Some(20.seconds)
  )

val config =
  ConfigFactory.parseString(
    s"""
      inputs: ["prefix.pub.service.one.1", "prefix.pub.service.two.1"]
      group-id:  "prefix.groupid.1"
      fake-poll-interval = 5s
      max-poll-interval: 10s
      max-poll-records: 1
      client-id: "custom-client-id"
      auto-commit: true
      auto-offset-reset: "latest"
      session-timeout: 2s
      enabled: false
    """
  )

KafkaConsumerConfigBuilder.buildFrom(config)
```

### KafkaProducerConfig

```scala mdoc:reset
import com.tenable.library.kafkaclient.config.builders.hocon.KafkaProducerConfigBuilder
import com.typesafe.config.ConfigFactory

val config =
  ConfigFactory.parseString(
    s"""
      acks: all
      retries: 20
      batch-size: 300
      buffer-size: 3000
      linger: 30s
    """
  )

KafkaProducerConfigBuilder.buildFrom(config)
```
