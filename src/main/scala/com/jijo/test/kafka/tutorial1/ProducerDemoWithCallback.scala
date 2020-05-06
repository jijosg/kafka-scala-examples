package com.jijo.test.kafka.tutorial1

import java.util.Properties

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.{Logger, LoggerFactory}

/**
 * Observe data using the following cli command
 * kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_topic --group my-first-application
 */
object ProducerDemoWithCallback extends App {

  val logger = LoggerFactory.getLogger(ProducerDemoWithCallback.getClass)
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
  producer.send(record, new Callback {
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
      if (exception == null) {
        logger.info(
          s"""
             |Recieved new metadata
             |Topic : ${metadata.topic()}
             |Partition: ${metadata.partition()}
             |Offset: ${metadata.offset()}
             |Timestamp: ${metadata.timestamp()}
             |""".stripMargin)
      }else{
        logger.error("Error while processing",exception)
      }

    }
  })

  // flush data
  producer.flush()

  // flush and close
  producer.close()
}
