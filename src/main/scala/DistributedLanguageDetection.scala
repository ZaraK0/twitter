import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.HashMap


object DistributedLanguageDetection {

  val stored_tweets_file_input = "input/tweets.txt"
  val stored_tweets_file = "output/tweets.txt"

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //    generate_tweets()
    distributed_kmeans()
  }

  def generate_tweets(): Unit = {
    // Twitter Stream ( return language of tweet alongside with tweet text for evaluation purposes )
    TwitterStreamingAPI.streamingClient.sampleStatuses(stall_warnings = true)(TwitterStreamingAPI.TweetTextLangToFile(stored_tweets_file, Set[String]("en", "es", "fr")))
  }

  def distributed_kmeans(): Unit = {
    // Spark Streaming Configuration
    val conf = new SparkConf().setAppName("DistributedLanguageDetection").setMaster("local[*]")
    val sc = new SparkContext(conf)

    var langs_arr = Array.fill[HashMap[String, Int]](3)(HashMap[String, Int]())

    // Load and parse the data
    val data = sc.textFile(stored_tweets_file_input)
    val parsedData = data.map(getText).map(s => Vectors.dense(TextProcessing.create_feature_vector(s))).cache()

    val tweets = data.collect().toList

    // Cluster the data into two classes using KMeans
    val numClusters = 3
    val numIterations = 1000
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)

    // Save and load model
    clusters.save(sc, "output/KMeansModel")
    val sameModel = KMeansModel.load(sc, "output/KMeansModel")

    // Evaluate
    tweets.foreach { tweet =>
      val predicted_class = sameModel.predict(Vectors.dense(TextProcessing.create_feature_vector(getText(tweet))))
      val language = getLanguage(tweet)

      if (langs_arr(predicted_class) contains language)
        langs_arr(predicted_class)(language) += 1
      else
        langs_arr(predicted_class) += (language -> 1)
    }

    for (lang_arr <- langs_arr) {
      var total = 0
      var max = Integer.MIN_VALUE
      var max_lang = ""

      lang_arr.foreach { tuple =>
        val language = tuple._1
        val count = tuple._2

        total += count
        if (count > max) {
          max = count
          max_lang = language
        }
      }

      if (total > 0)
        println("dominant language: " + max_lang + " --> accuracy: " + max.toFloat / total + " , max = " + max + ", total = " + total)
    }
    println("===========")

    sc.stop()
  }

  def getText(string: String): String = {
    val splitted = string.split(", ", 2)
    if (splitted.size == 2)
      splitted(1)
    else
      splitted(0)
  }

  def getLanguage(string: String): String = {
    val splitted = string.split(", ", 2)
    if (splitted.size == 2)
      splitted(0)
    else
      ""
  }
}
