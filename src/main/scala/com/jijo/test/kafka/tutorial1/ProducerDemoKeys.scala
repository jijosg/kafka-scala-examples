package com.jijo.test.kafka.tutorial1

import java.util.Properties

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

/**
 * Observe data using the following cli command
 * kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_topic --group my-first-application
 */
object ProducerDemoKeys extends App {

  val logger = LoggerFactory.getLogger(ProducerDemoWithCallback.getClass)
  val props: Properties = new Properties()

  //create Producer properties
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

  // create the producer
  val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)


  // send the data - asynchronous
  for (i <- 1 to 10) {

    val topic = "first_topic"
    val value = "hello_world  "+i
    val key = "id_"+i
    // create a producer record
    val record: ProducerRecord[String, String] =
      new ProducerRecord[String, String](topic,key,value)

    logger.info("Key "+key)
    producer.send(record, (metadata: RecordMetadata, exception: Exception) => {
        if (exception == null) {
          logger.info(
            s"""
               |Recieved new metadata
               |Topic : ${metadata.topic()}
               |Partition: ${metadata.partition()}
               |Offset: ${metadata.offset()}
               |Timestamp: ${metadata.timestamp()}
               |""".stripMargin)
        } else {
          logger.error("Error while processing", exception)
        }

      }
    ).get() //block the send() to make it synchronous - dont do it in production
  }

  // flush data
  producer.flush()

  // flush and close
  producer.close()
}
