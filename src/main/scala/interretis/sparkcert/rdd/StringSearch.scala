package interretis.sparkcert.rdd

import org.apache.spark.rdd.RDD

class StringSearch(val query: String) {

  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  def getMatchesFunctionReferece(rdd: RDD[String]): RDD[String] = {
    rdd filter isMatch
  }

  def getMatchesFieldReference(rdd: RDD[String]): RDD[String] = {
    rdd flatMap (x => x split query)
  }

  def getMatchesNoReference(rdd: RDD[String]): RDD[String] = {
    val q = this.query
    rdd flatMap (x => x split q)
  }
}
