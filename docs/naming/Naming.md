# Naming
This directory contains classes to store consumer group and topic names.

### ConsumerGroupName
This class has enforced rules for consumer group names.

```scala mdoc
import com.tenable.library.kafkaclient.naming.ConsumerGroupName

ConsumerGroupName("some.good.1")
ConsumerGroupName("some", "good", 1) // "some.good.1"
```

Bad name examples that will result in an `IllegalArgumentException`:
```scala mdoc:crash
ConsumerGroupName("No.Caps.1")
```
```scala mdoc:crash
ConsumerGroupName("not.good")
```
```scala mdoc:crash
ConsumerGroupName("n$.extraneou%.1")
```

Others that will fail:
`"", "not.good.enough", "1ddd.3dss.1", "no.valid.1.largesentence", "a.b.1"`

### TopicName

```scala mdoc
import com.tenable.library.kafkaclient.naming.TopicName

TopicName("good.priv.name.with.1")
TopicName("good", false, "name", "with", 1) // "good.priv.name.with.1"
```

Bad name examples that will result in an `IllegalArgumentException`:
```scala mdoc:crash
TopicName("No.Caps.Allowed.Here.1")
```
```scala mdoc:crash
TopicName("n$.extraneou%.s&mbols.allowed.1")
```
```scala mdoc:crash
TopicName("1dd.3dss.1.dd.ss.s.1")
```

Others that will fail:
`"", "not.good", "not.good.enough" 

