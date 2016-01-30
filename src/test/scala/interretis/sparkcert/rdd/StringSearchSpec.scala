package interretis.sparkcert.rdd

import interretis.sparktesting.SeparateContext
import org.apache.spark.SparkException
import org.scalatest.Matchers

class StringSearchSpec extends SeparateContext with Matchers {

  private val strings = List("one", "one two", "one two three")
  private val search = new StringSearch("three")

  "function reference" should "not work" in { f =>
    val lines = f.sc.parallelize(strings)
    val exception = the[SparkException] thrownBy search.getMatchesFunctionReferece(lines)
    exception.getMessage shouldBe "Task not serializable"
  }

  "field reference" should "not work" in { f =>
    val lines = f.sc.parallelize(strings)
    val exception = the[SparkException] thrownBy search.getMatchesFieldReference(lines)
    exception.getMessage shouldBe "Task not serializable"
  }

  "no reference" should "work" in { f =>
    val lines = f.sc.parallelize(strings)
    val result = search.getMatchesNoReference(lines)
    result.collect() should not be empty
  }
}
