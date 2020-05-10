package com.jijo.test.kafka.tutorial1

import java.time.Duration
import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory

object ConsumerDemo extends App {

  val logger = LoggerFactory.getLogger(ConsumerDemo.getClass)
  val bootstrapServers = "127.0.0.1:9092"
  val groupId = "my-first-application"
  val props = new Properties()
  props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
  props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  // create a consumer
  val consumer = new KafkaConsumer[String, String](props)

  // subscribe the consumer to topic(s)
  consumer.subscribe(util.Arrays.asList("first_topic"))

  // poll for new data
  while (true) {
    val consumerRecords: ConsumerRecords[String, String] =
      consumer.poll(Duration.ofMillis(100)) //new in Kafka 2.0.0
    consumerRecords.forEach(record => {
      logger.info("Key : " + record.key() + " value : " + record.value())
      logger.info("Partition: " + record.partition() + " offset: " + record.offset())
    }
    )
  }
}
