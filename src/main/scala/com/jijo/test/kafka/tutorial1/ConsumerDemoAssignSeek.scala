package com.jijo.test.kafka.tutorial1

import java.time.Duration
import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory

import scala.util.control.Breaks._

object ConsumerDemoAssignSeek extends App {

  val logger = LoggerFactory.getLogger(ConsumerDemo.getClass)
  val bootstrapServers = "127.0.0.1:9092"
  val props = new Properties()
  props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  // create a consumer
  val consumer = new KafkaConsumer[String, String](props)

  // assign and seek
  val paritionToReadFrom: TopicPartition = new TopicPartition("first_topic", 2)
  val offsetToReadFrom: Long = 2L
  consumer.assign(util.Arrays.asList(paritionToReadFrom))

  // seek
  consumer.seek(paritionToReadFrom, offsetToReadFrom)

  var numberOfMessagesToRead: Int = 5
  var keepOnReading: Boolean = true
  var numberOfMessagesReadSoFar: Int = 0

  // poll for new data
  breakable {

    while (keepOnReading) {
      val consumerRecords: ConsumerRecords[String, String] =
        consumer.poll(Duration.ofMillis(100)) //new in Kafka 2.0.0

      consumerRecords.forEach(record => {
        numberOfMessagesReadSoFar += 1
        logger.info("Key : " + record.key() + " value : " + record.value())
        logger.info("Partition: " + record.partition() + " offset: " + record.offset())
        if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
          keepOnReading = false
          break()
        }
      })
    }
  }

  logger.info("Exiting the application")

}
