package com.jijo.test.kafka.tutorial

import java.util.Properties

import org.apache.kafka.clients.producer._

object Producer {
  def main(args: Array[String]): Unit = {
    writeToKafka("quick_start")
  }
  def writeToKafka(topic: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)

    for (i <- 1 to 10) {
      val record = new ProducerRecord[String, String](topic, s"192.168.2.$i")
      producer.send(record)
    }

    producer.close()
  }
}
