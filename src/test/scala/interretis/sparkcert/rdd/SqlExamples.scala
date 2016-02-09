package interretis.sparkcert.rdd

import interretis.sparktesting.SeparateSqlContext
import org.scalatest.Matchers

class SqlExamples extends SeparateSqlContext with Matchers {

  "reading JSON" should "work with HiveContext" in { f =>
    val tweets = f.sqlCtx.read.json("src/main/resources/tweets.json")
    tweets.registerTempTable("tweets")
    val results = f.sqlCtx.sql("SELECT user.name, text FROM tweets")
    results.count() shouldBe 2
  }

  "example" should "work" in { f =>
    val rdd = f.sc.parallelize((1 to 100).map(i => Record(i, s"val_$i")))
    import f.sqlCtx.implicits._
    val df = rdd.toDF()
    df.registerTempTable("records")
    val records = f.sqlCtx.sql("SELECT * FROM records")
    records.collect() should have length 100
    val keys = records.map(row => s"${row(0)}")
    keys.collect() should have length 100
  }
}

case class Record(key: Int, value: String)
