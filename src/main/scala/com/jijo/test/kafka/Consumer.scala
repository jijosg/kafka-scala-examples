package com.jijo.test.kafka

import java.time.Duration
import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object Consumer{

  def main(args: Array[String]): Unit = {
    consumeFromKafka("quick_start")
  }
  def consumeFromKafka(topic: String) = {

    val bootstrapServers = "localhost:9092"
    val groupId = "my-first-application"

    //create consumer config
    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)

    //create consumer
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    //subscribe to our topic(s)
    consumer.subscribe(util.Collections .singleton(topic))

    //poll for new data
    while (true) {
      val records = consumer.poll(Duration.ofMillis(100)).asScala
      for (data <- records.iterator)
        println(s"Key : ${data.key} , Value : ${data.value} ,Partition : ${data.partition} , Offset : ${data.offset}")

    }
  }
}
