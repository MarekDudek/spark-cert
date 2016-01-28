package interretis.sparkcert.rdd

import interretis.sparktesting.SeparateContext
import org.scalatest.Matchers

class WordCountSpec extends SeparateContext with Matchers {

  "word-count" should "produce proper results" in { f =>
    val lines = f.sc.parallelize(List("one", "one two", "one two three"))
    val app = new WordCount
    val result = app wordCount lines
    val counts = result collectAsMap()
    counts shouldBe Map("one" -> 3, "two" -> 2, "three" -> 1)
  }
}
