package interretis.sparkcert.rdd

import interretis.sparktesting.SeparateSqlContext
import org.apache.spark.sql.Row
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

  "joining with SQL" should "work" in { f =>
    import f.sqlCtx.implicits.rddToDataFrameHolder

    // given
    val i1 = Item("1", "first", 1, "c1")
    val i2 = Item("2", "second", 2, "c2")
    val i3 = Item("3", "third", 3, "c3")
    val items = f.sc.parallelize(List(i1, i2, i3))
    items.toDF().registerTempTable("items")

    val c1 = Company("c1", "company-1", "city-1")
    val c2 = Company("c2", "company-2", "city-2")
    val companies = f.sc.parallelize(List(c1, c2))
    companies.toDF().registerTempTable("companies")

    // when
    val joined = f.sqlCtx.sql("SELECT * FROM companies c JOIN items i on c.companyId = i.companyId")

    // then
    joined.collect() should contain theSameElementsAs Seq(
      Row("c1", "company-1", "city-1", "1", "first", 1, "c1"),
      Row("c2", "company-2", "city-2", "2", "second", 2, "c2")
    )
  }
}

case class Record(key: Int, value: String)


//case class Item(id: String, name: String, unit: Int, companyId: String)

//case class Company(companyId: String, name: String, city: String)

