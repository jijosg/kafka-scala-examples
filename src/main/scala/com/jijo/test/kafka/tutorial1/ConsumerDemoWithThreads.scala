package com.jijo.test.kafka.tutorial1

import java.time.Duration
import java.util
import java.util.Properties
import java.util.concurrent.CountDownLatch

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory

object ConsumerDemoWithThreads extends App {

  val logger = LoggerFactory.getLogger(ConsumerDemo.getClass)
  val bootstrapServers = "127.0.0.1:9092"
  val groupId = "my-second-application"
  val topic = "first_topic"

  val latch: CountDownLatch = new CountDownLatch(1)
  // create the consumer runnable
  logger.info("Creating consumer")
  val consumerRunnable: Runnable = new ConsumerRunnable(bootstrapServers, groupId, topic, latch)
  val myThread: Thread = new Thread(consumerRunnable)
  myThread.start()

  // add a shutdown hook
  Runtime.getRuntime.addShutdownHook(new Thread(()=>{
    logger.info("Caught shutdown hook")

    try{
      latch.await()
    }catch {
      case e:InterruptedException => e.printStackTrace()
    }
    logger.error("Application exited")
  }))

  try {
    latch.await()
  } catch {
    case e: InterruptedException => logger.error("Application got interrupted ",e)
  } finally {
    logger.info("Application is closing")
  }
}

class ConsumerRunnable(val bootstrapServers: String, val groupId: String,
                       val topic: String, val latch: CountDownLatch) extends Runnable {
  val logger = LoggerFactory.getLogger(classOf[ConsumerRunnable])

  val props = new Properties()
  props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
  props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  // create a consumer
  private val consumer = new KafkaConsumer[String, String](props)
  // subscribe the consumer to topic(s)
  consumer.subscribe(util.Arrays.asList("first_topic"))

  def run() = {

    // poll for new data
    try {
      while (true) {
        val consumerRecords: ConsumerRecords[String, String] =
          consumer.poll(Duration.ofMillis(100)) //new in Kafka 2.0.0
        consumerRecords.forEach(record => {
          logger.info("Key : " + record.key() + " value : " + record.value())
          logger.info("Partition: " + record.partition() + " offset: " + record.offset())
        }
        )
      }
    } catch {
      case e: WakeupException => logger.info("Recieved shutdown signal")
    } finally {
      consumer.close()
      latch.countDown()
    }
  }

  def shutdown() = {
    // the wakeup() method is a special method to interrupt consumer.poll
    // it will throw an exception WakeupException
    consumer.wakeup()
  }
}
