---
layout: page
title: "Configuration"
section: "config"
position: 2
---

# Configuration

This directory contains classes for kafka producer, consumer and topic configs. Helpful builder methods are also
available.

### KafkaConnectionString

```scala
import com.tenable.library.kafkaclient.config.KafkaConnectionString
import com.tenable.library.kafkaclient.config.builders.hocon.KafkaConnectionStringBuilder
import com.typesafe.config.ConfigFactory


val config = ConfigFactory.parseString(s"""brokers: ["localhost:6001"]""")

KafkaConnectionStringBuilder.buildFrom(config)
```
