package com.jijo.test.kafka.tutorial1

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

/**
 * Observe data using the following cli command
 * kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_topic --group my-first-application
 */
object ProducerDemo extends App {

  val props: Properties = new Properties()

  //create Producer properties
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

  // create the producer
  val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)

  // create a producer record

  val record: ProducerRecord[String, String] =
    new ProducerRecord[String, String]("first_topic", "hello_world")

  // send the data - asynchronous
  producer.send(record)

  // flush data
  producer.flush()

  // flush and close
  producer.close()
}
