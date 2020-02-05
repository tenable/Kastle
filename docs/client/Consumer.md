# KafkaConsumerIO

### Instantiating a kafka consumer

```scala mdoc:compile-only
import cats.effect.IO
import scala.concurrent.ExecutionContext.global
import com.tenable.library.kafkaclient.config.KafkaConsumerConfig
import com.tenable.library.kafkaclient.client.standard.consumer.actions.ProcessAction
import com.tenable.library.kafkaclient.client.standard.KafkaConsumerIO
import scala.concurrent.duration._
import org.apache.kafka.common.serialization.StringDeserializer

implicit val T = IO.timer(global)
implicit val CS = IO.contextShift(global)
implicit val CE = IO.ioConcurrentEffect(CS)

val kafkaConnectionString: String = "127.0.0.1:9"
val topics = Set("prefix.priv.service.thetopic.1")
val consumerGroup = "prefix.group.1"

val config = KafkaConsumerConfig(kafkaConnectionString, topics, consumerGroup, 10.seconds)

val consumerResource = KafkaConsumerIO
  .builder[IO, String, String](config)
  .withKeyDeserializer(new StringDeserializer)
  .withValueDeserializer(new StringDeserializer)
  .resource

consumerResource.use { consumer =>
    //Use the consumer with all the methods provided
    //consumer.poll()
    //consumer.pause()
    //consumer.commit(...)
    ???
}
```

### Setting up kafka consumers to poll forever

KafkaRunLoop.Builder, offers several customizations available:
- `consuming`: To specify how the polled batch is to be consumed
- `consumingSingleEvents`: Handy shortcut to consume event by event
- `consumingTopicPartitionBatch`: Handy shortcut to consume all the events for a Topic Partition in one go
- `consumingFullBatch`: Handy shortcut to consume a full polled batch

- `expecting`: To specify what the result type of the processing action would be
- `expectingEither`: Handy shortcut. This will allow you to have your processing function to return Either[E, Unit], if Right it will commit, if left it will reject.
- `expectingTry`: Handy shortcut. This will allow you to have your processing function to return Try[Unit]
- `expectingProcessAction`: Handy shortcut. This will allow you to have your processing function to return ProcessAction. Which is the most flexible included return type

- `withRebalanceDetector`: If set and a rebalance occurs while your *cancelable* processing function runs, it will cancel it. Might be handy for long running processing functions.

At least a `consumingXXX` and a `expectingXXX` functions must be called before calling the function run.

Example:

```scala mdoc:compile-only
consumerResource.use { consumer =>
  consumer
    .pollForever
    .consumingSingleEvents
    .expectingProcessAction
    .run(1.second) { record =>
        IO.delay(println(record.value)).map(_ => ProcessAction.commitAll)
    } //This returns a CancelToken, in case you wish to cancel
}
```

## Customizing the Action of the consumer loop

There is no need to explicitly call `ProcessAction.commitAll`, you can define your own custom reactions depending on the processing returned value or use one of the predefined handlers for the most common responses.

Below is a full example using different `EventActionable`s:

### Either response
```scala mdoc:compile-only
import com.tenable.library.kafkaclient.client.standard.consumer.EventActionable

// Committing based on either
import cats.instances.string._
consumerResource.use { consumer =>
  consumer
    .pollForever
    .consumingSingleEvents
    .expectingEither[String]
    .run(1.second) { record =>
        IO.delay(println(record.value)).map(_ => Right(())) //To force commit. Use left to reject.
    }
}
```

### Try response
```scala mdoc:compile-only
// Committing based on try
implicit val showThrowable = cats.Show.fromToString[Throwable]
consumerResource.use { consumer =>
  consumer
    .pollForever
    .consumingSingleEvents
    .expectingTry
    .run(1.second) { record =>
        IO.delay(println(record.value)).map(_ => scala.util.Try(()))
    }
}
```

### Any `G[R]` for which `G: ApplicativeError[?[_], E]: Foldable` and `E: Show`

Either and Try are implemented using this approach.
See `EventActionable.scala` for details

### Custom defined handler
```scala mdoc:compile-only
// Committing based on result
implicit val eventActionableFromResult = EventActionable.deriveFromResult[Option[String]] {
  case Some(thing) if thing == "do-commit" => ProcessAction.commitAll
  case Some(other)                         => ProcessAction.rejectAll(other)
  case None                                => ProcessAction.rejectAll("nope")
}
consumerResource.use { consumer =>
  consumer.pollForever.consumingSingleEvents.expecting[Option[String]].run(1.second) { record =>
    IO.delay(println(record.value)).map(_ => Some("commit this"))
  }
}
```

### The most flexible: ProcessAction
```scala mdoc:compile-only
// Committing based on returned process action
consumerResource.use { consumer =>
  consumer
    .pollForever
    .consumingSingleEvents
    .expectingProcessAction
    .run(1.second) { record =>
        IO.delay(println(record.value)).map(_ => ProcessAction.commitAll)
    }
}
```
