package  com.jijo.test.kafka.tutorial2

import java.util.Properties
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}

import com.google.common.collect.Lists
import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.core.{Client, Constants, Hosts, HttpHosts}
import com.twitter.hbc.httpclient.auth.{Authentication, OAuth1}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.slf4j.LoggerFactory

object TwitterProducer extends App {
  val consumerKey = args(0)
  val consumerSecret = args(1)
  val token = args(2)
  val secret = args(3)

  val logger = LoggerFactory.getLogger(TwitterProducer.getClass)
  //create a twitter client
  TwitterProducer.run()

  def run():Unit = {
    println("Setup")
    val msgQueue: BlockingQueue[String] = new LinkedBlockingQueue[String](1000)
    val client: Client = createTwitterClient(msgQueue)
    client.connect()

    //create kafka producer
    val producer:KafkaProducer[String,String] = createKafkaProducer()

    /*//add a shutdown hook
    Runtime.getRuntime.addShutdownHook(new Thread {
      println("Shutting down the application")
      client.stop()
      producer.close()
    })*/
    while (!client.isDone) {
      var msg: String = ""
      try {
        msg = msgQueue.poll(5, TimeUnit.SECONDS)
      } catch {
        case e: InterruptedException => e.printStackTrace(); client.stop()
      }
      if (msg != "") {
        logger.info(msg)
        /*producer.send(new ProducerRecord("twitter_tweets",msg),
          (metadata: RecordMetadata, exception: Exception) => exception match {
            case x => logger.error("Something bad happened",x)
          }

      )*/
      }
    }
    logger.info("End of execution!")
  }

  def createKafkaProducer():KafkaProducer[String,String] = {
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
    val hosebirdAuth: Authentication = new OAuth1(consumerKey, consumerSecret, token, secret)


    val builder: ClientBuilder = new ClientBuilder()
      .name("hosebird-Client-01") // optional: mainly for the logs
      .hosts(hosebirdHosts)
      .authentication(hosebirdAuth)
      .endpoint(hosebirdEndpoint)
      .processor(new StringDelimitedProcessor(msgQueue))
    val hosebirdClient: Client = builder.build()
    hosebirdClient

  }

}
