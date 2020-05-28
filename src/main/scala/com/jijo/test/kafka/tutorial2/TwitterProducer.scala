package com.jijo.test.kafka.tutorial2

import java.util.Properties
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}

import com.google.common.collect.Lists
import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.core.{Client, Constants, Hosts, HttpHosts}
import com.twitter.hbc.httpclient.auth.{Authentication, OAuth1}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.slf4j.LoggerFactory

/**
 * kafka-topics.sh --zookeeper localhost:2181  --create  --topic twitter_tweets --partitions 6 --replication-factor 1
 * kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic twitter_tweets
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
    val producer: KafkaProducer[String, String] = createKafkaProducer()

    // add a shutdown hook
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      logger.info("Stopping Application...")
      logger.info("Shutting down client from twitter...")
      client.stop()
      logger.info("Closing producer...")
      producer.close()
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
        val record: ProducerRecord[String, String] =
          new ProducerRecord[String, String]("twitter_tweets", null, msg)

        producer.send(record,
          (metadata: RecordMetadata, exception: Exception) => {
            if (exception != null) logger.error("Something bad happened", exception)
          })
      }
    }
  }

  def createKafkaProducer(): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    // create safe producer
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.RETRIES_CONFIG, Int.MaxValue.toString)
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5") // for kafka >1.1 use 5 , use 1 otherwise

    // high throughput producer ( at the expense of a bit of latency and CPU usage)
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
    props.put(ProducerConfig.LINGER_MS_CONFIG, "20")
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, s"${32 * 1024}")

    //create kafka producer
    val producer = new KafkaProducer[String, String](props)
    producer
  }

  def createTwitterClient(msgQueue: BlockingQueue[String]): Client = {

    val hosebirdHosts: Hosts = new HttpHosts(Constants.STREAM_HOST)
    val hosebirdEndpoint: StatusesFilterEndpoint = new StatusesFilterEndpoint()

    val terms = Lists.newArrayList("bitcoin", "usa", "politics", "sports", "soccer")
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
