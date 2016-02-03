package interretis.sparkcert.rdd

import interretis.sparktesting.SeparateSqlContext
import org.scalatest.Matchers

class SqlExamples extends SeparateSqlContext with Matchers {

  "reading JSON" should "work with HiveContext" in { f =>
    val tweets = f.sql.jsonFile("src/main/resources/tweets.json")
    tweets.registerTempTable("tweets")
    val results = f.sql.sql("SELECT user.name, text FROM tweets")
    results.collect().foreach(println)
  }
}
