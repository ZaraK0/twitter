import com.danielasfregola.twitter4s.entities.Tweet
import collection.mutable.HashMap
import math.{pow, sqrt}

class StreamingKMeans(k_input: Int) {
  var centroids = Array[Array[Double]]()
  var n_arr = Array[Int]()
  var langs_arr = Array.fill[HashMap[String, Int]](3)(HashMap[String, Int]())

  val alpha = 0.5

  var language_counter = HashMap[String, Int]("en" -> 0, "es" -> 0, "fr" -> 0)

  val counter_threshold = 3

  var k = k_input

  def setK(k: Int): Unit = {
    this.k = k
  }

  def addCenter(centroid: Array[Double]): Unit = {
    centroids = centroids :+ centroid
    n_arr = n_arr :+ 1
  }

  def getDistance(point1: Array[Double], point2: Array[Double]): Double = {
    if (point1.size != point2.size) {
      throw new Exception("Distance between two points in different dimensions")
    }

    var squared_sum = 0.0

    for (i <- 0 until point1.size) {
      squared_sum += pow(point1(i) - point2(i), 2)
    }
    sqrt(squared_sum)
  }

  def sum_elementwise_arrays(arr1: Array[Double], arr2: Array[Double]): Array[Double] = {
    (arr1, arr2).zipped.map(_ + _)
  }

  def mul_elementwise_array_number(arr1: Array[Double], number: Double): Array[Double] = {
    (arr1, Array.fill[Double](arr1.size)(number)).zipped.map(_ * _)
  }

  def divide_elementwise_array_number(arr1: Array[Double], number: Double): Array[Double] = {
    (arr1, Array.fill[Double](arr1.size)(number)).zipped.map(_ / _)
  }

  def updateClusters(tweets: List[String]): Unit = {

    var batch = tweets.map(_.split(", ", 2)(1)).map(TextProcessing.create_feature_vector)
    var batch_languages = tweets.map(_.split(", ", 2)(0))

    var sum = Array.fill[Array[Double]](centroids.size)(Array.fill[Double](TextProcessing.ngram_features.size)(0.0))
    var m_arr = Array.fill[Int](centroids.size)(0)

    for (i <- 0 until batch.size) {
      val data = batch(i)
      val language = batch_languages(i)

      if (language == "en" || language == "es" || language == "fr") {

        // Control distribution of tweets
        if (language_counter(language) <= Math.min(language_counter("en"), Math.min(language_counter("es"), language_counter("fr"))) + counter_threshold ) {
          language_counter(language) += 1

          var min_dist = Double.MaxValue
          var min_index = -1

          // Find nearest centroid
          for (i <- 0 until centroids.size) {
            val centroid = centroids(i)
            val dist = getDistance(data, centroid)
            if (dist < min_dist) {
              min_dist = dist
              min_index = i
            }
          }

          if (min_index != -1) {
            if (langs_arr(min_index) contains language)
              langs_arr(min_index)(language) += 1
            else
              langs_arr(min_index) += (language -> 1)

            sum(min_index) = (sum(min_index), data).zipped.map(_ + _)
            m_arr(min_index) += 1
          }
        }
      }
    }

    // Update Centroids
    for (i <- 0 until centroids.size) {
      centroids(i) =  divide_elementwise_array_number(
                        sum_elementwise_arrays(
                          mul_elementwise_array_number(
                            centroids(i),
                            n_arr(i) * alpha),
                          sum(i)),
                        n_arr(i) * alpha + m_arr(i))

      n_arr(i) = n_arr(i) + m_arr(i)
    }
  }

  def printCentroids(): Unit = {
    println("number of centroids " + centroids.size.toString)
    for (centroid <- centroids) {
      println("hi" + centroid.size.toString)
      for (item <- centroid) {
        println(item)
      }
      println("=================")
    }
  }

  def printAccuracy(): Unit = {
    println("total en " + language_counter("en"))
    println("total es " + language_counter("es"))
    println("total fr " + language_counter("fr"))

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
  }
}
