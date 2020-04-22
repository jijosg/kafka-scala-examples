package com.jijo.test.kafka

import java.time.Duration
import java.util
import java.util.Properties
import java.util.concurrent.CountDownLatch

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object ConsumerDemoWithThread{


  def main(args: Array[String]): Unit = {
    consumeFromKafka("quick_start")
  }
  def consumeFromKafka(topic: String) = {

    val bootstrapServers = "localhost:9092"
    val groupId = "my-sixth-application"
    val topic = "first-topic"
    val latch:CountDownLatch = new CountDownLatch(1)

    val myConsumerThread:Runnable =new ConsumerThread(bootstrapServers,
      groupId,
      topic,
      latch
    )

    val thread:Thread = new Thread(myConsumerThread)
    thread.run()
    //
    Runtime.getRuntime.addShutdownHook(new Thread{
      println("Caught shutdown hook")
      myConsumerThread.shutdown()
      latch.await()
    })
    latch.await()

  }
}

abstract class ConsumerThread extends Runnable{
  var latch:CountDownLatch
  var consumer:KafkaConsumer[String,String]
  def ConsumerThread(bootstrapServers:String,groupId:String,topic:String,latch:CountDownLatch): Unit ={
    this.latch = latch
    //create consumer config
    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)

    consumer = new KafkaConsumer[String, String](props)
    //subscribe to our topic(s)
    consumer.subscribe(util.Collections .singleton(topic))
  }
  override def run(): Unit = {
    try { //poll for new data
      while (true) {
        val records = consumer.poll(Duration.ofMillis(100)).asScala
        for (data <- records.iterator)
          println(s"Key : ${data.key} , Value : ${data.value} ,Partition : ${data.partition} , Offset : ${data.offset}")

      }
    }catch{
      case exc : WakeupException => exc.printStackTrace()
    }
  }
  def shutdown():Unit = {
    consumer.wakeup()
  }
}
