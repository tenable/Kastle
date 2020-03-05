package com.tenable.library.kafkaclient.config

import java.util.Properties

import cats.implicits._
import com.github.ghik.silencer.silent
import org.apache.kafka.clients.producer.ProducerConfig

import scala.concurrent.duration._
import scala.collection.JavaConverters._
import scala.collection.Map
import scala.util.Random

sealed abstract class AckStrategy(private[config] val configValue: String) {
  override def toString: String = configValue
}

object AckStrategy {
  object NoAck extends AckStrategy("0")
  object One   extends AckStrategy("1") //only leader acks
  object All   extends AckStrategy("all") //every broker must ack

  def fromString(str: String): Either[String, AckStrategy] = {
    str match {
      case NoAck.configValue => NoAck.asRight
      case One.configValue   => One.asRight
      case All.configValue   => All.asRight
      case _                 => s"Unknown ack strategy $str".asLeft
    }
  }
}
case class KafkaProducerConfig(
    connectionString: String,
    ackStrategy: AckStrategy,
    retries: Int,                             //Have client retry failed sends -- subtle pitfall: may mess up ordering -- see ProducerConfig.RETRIES_DOC
    batchSize: Int,                           //in bytes
    linger: FiniteDuration,                   //wait up to this long to receive batchsize worth of messages before sending
    bufferSize: Long,                         //memory to use for records waiting to be sent
    additionalProperties: Map[String, String] //properties in here override ones set by above variables
) {

  @silent
  def properties[K, V]: Properties = {
    val props = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connectionString)
    //TODO: instead of hostname, configure this portion of client id from config?
    props.put(
      ProducerConfig.CLIENT_ID_CONFIG,
      s"producer-${Option(System.getenv("HOSTNAME")).getOrElse("UNKNOWN")}-${Random.nextInt}"
    )
    props.put(ProducerConfig.ACKS_CONFIG, ackStrategy.configValue)
    props.put(ProducerConfig.RETRIES_CONFIG, new Integer(retries))
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, new Integer(batchSize))
    props.put(ProducerConfig.LINGER_MS_CONFIG, new Integer(linger.toMillis.toInt))
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, new java.lang.Long(bufferSize.toString))
    additionalProperties.foreach { case (k, v) => props.setProperty(k, v) }
    props
  }
}

object KafkaProducerConfig {
  @silent
  def apply(
      connectionString: String,
      ackStrategy: AckStrategy,
      retries: Int,
      batchSize: Int,
      linger: FiniteDuration,
      bufferSize: Long,
      additionalProperties: Properties
  ): KafkaProducerConfig = {
    new KafkaProducerConfig(
      connectionString,
      ackStrategy,
      retries,
      batchSize,
      linger,
      bufferSize,
      Map(additionalProperties.asScala.toSeq: _*) //copy this because additionalProperties is mutable
    )
  }

  def default(connectionString: String): KafkaProducerConfig =
    new KafkaProducerConfig(
      connectionString,
      ackStrategy = AckStrategy.One,
      retries = 0,                     //if not configured with max batch in flight = 1, retries can mess up ordering.  disable by default for now
      batchSize = 64 * 1024,           //arbitrarily settings this to 64k for now
      linger = 100.millis,             //TODO tune this value
      bufferSize = 1024L * 1024L * 5L, //TODO: set this to some sane value
      additionalProperties = Map.empty[String, String]
    )

}
