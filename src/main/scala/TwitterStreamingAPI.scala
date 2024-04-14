import java.io.FileWriter

import com.danielasfregola.twitter4s.TwitterRestClient
import com.danielasfregola.twitter4s.TwitterStreamingClient
import com.danielasfregola.twitter4s.entities.Tweet
import com.danielasfregola.twitter4s.entities.streaming.StreamingMessage


object TwitterStreamingAPI {

  val restClient = TwitterRestClient()
  val streamingClient = TwitterStreamingClient()

  def printTweetText: PartialFunction[StreamingMessage, Unit] = {
    case tweet: Tweet =>
      println(tweet.text)
      println("================================")
  }

  def TweetTextToKafka(topic: String): PartialFunction[StreamingMessage, Unit] = {
    case tweet: Tweet => KafkaProducer.produce(tweet.text, topic)
  }

  def TweetTextLangToKafka(topic: String): PartialFunction[StreamingMessage, Unit] = {
    case tweet: Tweet => KafkaProducer.produce(tweet.lang.get + ", " + tweet.text, topic)
  }

  def TweetTextLangToFile(file: String, languages: Set[String]): PartialFunction[StreamingMessage, Unit] = {
    case tweet: Tweet => {
      if (languages contains tweet.lang.get) {
        println(tweet.lang.get + ", " + tweet.text)
        println("================================")
        val fw = new FileWriter(file, true)
        try {
          fw.write(tweet.lang.get + ", " + tweet.text.replace("\n", " ") + "\n")
          println("write!")
        }
        finally {
          fw.close()
        }
      }
    }
  }

}
