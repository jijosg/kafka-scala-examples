package com.jijo.test.kafka.tutorial2

import java.util.Properties
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}

import com.google.common.collect.Lists
import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.core.{Client, Constants, Hosts, HttpHosts}
import com.twitter.hbc.httpclient.auth.{Authentication, OAuth1}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.slf4j.LoggerFactory

/**
 * kafka-topics.sh --zookeeper localhost:2181  --create  --topic twitter_tweets --partition 6 --replication-factor 1
 *
 */
object TwitterProducer extends App {
  val consumerKey = args(0)
  val consumerSecret = args(1)
  val token = args(2)
  val secret = args(3)

  val logger = LoggerFactory.getLogger(TwitterProducer.getClass)

  //create a twitter client
  TwitterProducer.run()

  def run(): Unit = {
    logger.info("Setup")
    val msgQueue: BlockingQueue[String] = new LinkedBlockingQueue[String](1000)

    // connect to twitter client
    val client: Client = createTwitterClient(msgQueue)
    client.connect()

    //create kafka producer
//    val producer: KafkaProducer[String, String] = createKafkaProducer()

    // add a shutdown hook
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      logger.info("Stopping Application...")
      logger.info("Shutting down client from twitter...")
      client.stop()
//      logger.info("Closing producer...")
//      producer.close()
      logger.info("Done!")
    }))

    while (!client.isDone) {
      var msg: String = null
      try {
        msg = msgQueue.poll(5, TimeUnit.SECONDS)
      } catch {
        case e: InterruptedException => e.printStackTrace(); client.stop()
      }
      if (msg != null) {
        logger.info(msg)
        /*val record: ProducerRecord[String, String] =
          new ProducerRecord[String, String]("twitter_tweets",null,msg)

        producer.send(record,
          (metadata: RecordMetadata, exception: Exception) => {
            if (exception != null) logger.error("Something bad happened" , exception)
          })*/
      }
    }
  }

  def createKafkaProducer(): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    producer
  }

  def createTwitterClient(msgQueue: BlockingQueue[String]): Client = {

    val hosebirdHosts: Hosts = new HttpHosts(Constants.STREAM_HOST)
    val hosebirdEndpoint: StatusesFilterEndpoint = new StatusesFilterEndpoint()

    val terms = Lists.newArrayList("bitcoin")
    hosebirdEndpoint.trackTerms(terms)
    // These secrets should be read from a config file
    val hosebirdAuth: Authentication
                    = new OAuth1(consumerKey, consumerSecret, token, secret)


    val builder: ClientBuilder = new ClientBuilder().name("hosebird-Client-01") // optional: mainly for the logs
                                                    .hosts(hosebirdHosts)
                                                    .authentication(hosebirdAuth)
                                                    .endpoint(hosebirdEndpoint)
                                                    .processor(new StringDelimitedProcessor(msgQueue))
    val hosebirdClient: Client = builder.build()
    hosebirdClient
  }

}
