import java.util.regex.{Matcher, Pattern}

/**
  * Created by shayansalehian on 3/8/17.
  */
object TextProcessing {

  var ngram_features = Array(
    "the", "ing", "you", "and", "for", "in", "th", "an", "er", "re", // English
    "que", "est", "ent", "con", "sta", "es", "en", "de", "er", "ar", // Spanish
    "ent", "our", "que", "est", "les", "es", "le", "en", "on", "ou" // French
  )

  def create_feature_vector(string: String) : Array[Double] = {
    var feature_vector = Array.fill[Double](ngram_features.size)(0.0) // Initialize with zeros

    var bigrams = create_ngrams(string, 2)
    for (bigram <- bigrams) {
      var i = 0
      while (i < ngram_features.size) {
        val ngram_feature = ngram_features(i)

        if (bigram.size == 2 && bigram == ngram_feature) {
          feature_vector(i) += 1
        }

        i += 1
      }
    }

    var trigrams = create_ngrams(string, 3)
    for (trigram <- trigrams) {
      var i = 0
      while (i < ngram_features.size) {
        val ngram_feature = ngram_features(i)

        if (trigram.size == 3 && trigram == ngram_feature) {
          feature_vector(i) += 1
        }

        i += 1
      }
    }

    feature_vector
  }

  def create_ngrams(line : String, n: Int) : Array[String] = {
    removeUrl(removeRetweet(line)).toLowerCase.replaceAll("""[\p{Punct}&&[^.]]""", "").split("[\\p{Punct}\\s]+").flatMap(word => word.sliding(n))
  }

  def removeUrl(string: String) : String = {
    var commentstr = string
    var urlPattern = "((https?|ftp|gopher|telnet|file|Unsure|http):((//)|(\\\\))+[\\w\\d:#@%/;$~_?\\+-=\\\\\\.&]*)"
    var p = Pattern.compile(urlPattern, Pattern.CASE_INSENSITIVE)
    var m = p.matcher(commentstr)
    var i = 0
    while (m.find()) {
      commentstr = commentstr.replaceAll(m.group(i),"").trim()
      i += 1
    }

    commentstr
  }

  def removeRetweet(string: String) : String = {
    if (string.startsWith("RT"))
      string.substring(2)
    else
      string
  }
}
