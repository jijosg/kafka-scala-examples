package  com.jijo.test.kafka.tutorial2

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}

import com.google.common.collect.Lists
import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.core.{Client, Constants, Hosts, HttpHosts}
import com.twitter.hbc.httpclient.auth.{Authentication, OAuth1}

object TwitterProducer extends App {

  //create a twitter client
  TwitterProducer.run()

  def run():Unit = {
    println("Setup")
    val msgQueue: BlockingQueue[String] = new LinkedBlockingQueue[String](1000)
    val client: Client = createTwitterClient(msgQueue)
    client.connect()

    while (!client.isDone) {
      var msg: String = ""
      try {
        msg = msgQueue.poll(5, TimeUnit.SECONDS)
      } catch {
        case e: InterruptedException => e.printStackTrace(); client.stop()
      }
      if (msg != "") {
        println(msg)
      }
    }
    println("End of execution!")
  }

  def createTwitterClient(msgQueue: BlockingQueue[String]): Client = {
    val consumerKey = "WsAeTgDSBnIwBZWw6KaijPN9b"
    val consumerSecret = "BfCqpAJiAQRGRl8PAo964CNEG6eck6lV6kGndZypel4"
    val token = "2932095118-XtnODAvArwq61KA8ZnirEgBBchd66mWwQIwpblO"
    val secret = "5AMehVYQH9wr17pGPJjavQS2CFJribm0l2YGWO3zJ2uDS"

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
