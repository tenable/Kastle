package com.tenable.library.kafkaclient.serialization

import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.Deserializer
import java.util
import org.apache.kafka.common.errors.SerializationException
import scala.util.control.NonFatal

abstract class SerializerBase[T](from: (String, T) => Array[Byte]) extends Serializer[T] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
  override def serialize(topic: String, data: T): Array[Byte] = {
    try {
      from(topic, data)
    } catch {
      case NonFatal(e) =>
        throw new SerializationException(s"Serialization failed for event $data in topic $topic", e)
    }
  }
  override def close(): Unit = ()
}

abstract class DeserializerBase[T](to: (String, Array[Byte]) => T) extends Deserializer[T] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
  override def deserialize(topic: String, data: Array[Byte]): T = {
    try {
      to(topic, data)
    } catch {
      case NonFatal(e) =>
        throw new SerializationException(
          s"Deserialization failed for event ${util.Arrays.toString(data)} in topic $topic",
          e
        )
    }
  }
  override def close(): Unit = ()
}
