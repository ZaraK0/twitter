import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}


object StreamingLanguageDetection {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Spark Streaming Configuration
    val conf = new SparkConf().setAppName("StreamingLanguageDetection").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(1)) // Spark Streaming Context, Batch Every 1 Second

    // Kafka Configuration
    val kafkaParams = Map("bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = Set("twitter-topic")

    // Spark Streaming and Kafka Integration
    val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams)) // ConsumerRecord

    val k = 3
    var current_k = 0
    val streaming_kmeans = new StreamingKMeans(k);

    stream.foreachRDD { rdd =>

      // Batch of tweets
      var tweets = rdd.map(_.value).collect().toList

      if (current_k < k) {
        for (tweet <- tweets) {
          var data = TextProcessing.create_feature_vector(tweet)

          if (current_k < k) {
            streaming_kmeans.addCenter(data)
            current_k += 1
          }
        }
      }

      streaming_kmeans.updateClusters(tweets)
//      streaming_kmeans.printCentroids()
      streaming_kmeans.printAccuracy()
    }

    ssc.start() // Start Streaming Thread

    // Twitter Stream ( return language of tweet alongside with tweet text for evaluation purposes )
    TwitterStreamingAPI.streamingClient.sampleStatuses(stall_warnings = true)(TwitterStreamingAPI.TweetTextLangToKafka("twitter-topic"))

    ssc.awaitTermination()
  }
}
