---
layout: page
title: "Producer"
section: "producer"
position: 4
---

# KafkaProducerIO

## Configure and instantiate the consumer
With a configured an instance of `KafkaProducerIO`, we can produce to any topic via the `send` and `unsafeSend`
methods.  Both are asynchronous, however, `send` will return when the message delivery is acknowledged, while `unsafeSend`
when the message is in the internal kafka producer buffer

If we only want to send to a fixed topic, we can do `kafkaProducer.forTopic[T]` which will give us a `KafkaProducerIO#ForTopic[T]`.
`T` is optional to ensure compilation time safety to send to the right topic.
It has similar methods but without the topic parameter

```scala
import com.tenable.library.kafkaclient.client.standard.KafkaProducerIO
import com.tenable.library.kafkaclient.config.KafkaProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import cats.effect.IO
import cats.synxtax.apply._
import scala.concurrent.ExecutionContext.global

implicit val CS = IO.contextShift(global)
implicit val CE = IO.ioConcurrentEffect(CS)

val kafkaConnectionString = "127.0.0.1:9000"

val producerResource = KafkaProducerIO
  .builder[IO, String, String](KafkaProducerConfig.default(kafkaConnectionString))
  .withKeyDeserializer(classOf[StringSerializer])
  .withValueDeserializer(classOf[StringSerializer])
  .resource

producerResource.use { producer =>
    //Use the producer
    ???
}
```

## Using the producer

```scala
val topic = "prefix.priv.service.thetopic.1"
val topic2 = "prefix.priv.service2.thetopic.1"

producerResource.use { kafkaProducer =>
  kafkaProducer.send(topic, "key", "value") *>
    kafkaProducer.sendAndForget(topic2, "key", "value")
}
```

## Using the producer with a fixed topic
```scala
trait MyTopic

val fixedTopic = "prefix.priv.service.thetopic.1"
producerResource.map { kafkaProducer =>
  kafkaProducer.forTopic[MyTopic](fixedTopic)
}.use { fixedKafkaProducer =>
  fixedKafkaProducer.send("key", "value") *>
    fixedKafkaProducer.unsafeSend("key", "value")
}
```