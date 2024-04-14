import com.danielasfregola.twitter4s.entities.enums.Language
import org.apache.kafka.common.serialization.StringDeserializer
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.{State, StateSpec}

import java.io._

import collection.JavaConversions._

object CommonNgrams {

//  locally {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Spark Streaming Configuration
    val conf = new SparkConf().setAppName("CommonNgrams").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Kafka Configuration
    val kafkaParams = mapAsJavaMap(Map[String, Object]("bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)))
//  }

  def main(args: Array[String]): Unit = {
    var languages = Array(Language.English)

    var language = languages(0)
//    for (language <- languages) {
      // Start recieving tweets
      TwitterStreamingAPI.streamingClient.sampleStatuses(languages = Seq(language), stall_warnings = true)(TwitterStreamingAPI.TweetTextToKafka("twitter-topic"))
      getCommonNgramsInLanguage(language, Array(2, 3)) // Bi-grams and Tri-grams
//    }
  }

  def getCommonNgramsInLanguage(language: Language.Language, ngrams: Array[Int]): Unit = {

    val offsetRanges = Array(
      // topic, partition, inclusive starting offset, exclusive ending offset
      OffsetRange("twitter-topic" , 0, 0, 5000)
    )

    val batch_tweets_rdd = KafkaUtils.createRDD[String, String](sc, kafkaParams, offsetRanges, PreferConsistent).map(_.value) // ConsumerRecord
    batch_tweets_rdd.saveAsTextFile("output/CommonNgrams/" + "tweets-" + language.toString)

//    val batch_tweets_rdd = sc.textFile("output/CommonNgrams/" + "tweets-" + language.toString + ".txt") // Load!

    for (n <- ngrams) {
      var ngrams_count = batch_tweets_rdd.flatMap { line =>
        var bigrams = TextProcessing.create_ngrams(line, n)
        bigrams
      }.map(ngram => (ngram, 1)).reduceByKey(_ + _).collect().toList

      ngrams_count = ngrams_count.sortBy(_._2)(Ordering[Int].reverse)

      val p = new java.io.PrintWriter(new File("output/CommonNgrams/" + language.toString + n.toString + ".txt"))

      for (i <- 0 to 20) {
        p.println(ngrams_count(i))
      }

      p.close()
    }

    System.exit(0) // To exit program and close twitter streaming
  }
}
