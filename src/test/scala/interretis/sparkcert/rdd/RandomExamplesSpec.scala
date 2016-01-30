package interretis.sparkcert.rdd

import interretis.sparktesting.SeparateContext
import org.scalatest.Matchers

class RandomExamplesSpec extends SeparateContext with Matchers {

  "example with flatMap" should "work" in { f =>
    val lines = f.sc.parallelize(Array("hello we are learning Spark from someplace", "We are also learning Hadoop from someplace"))
    val flattened = lines flatMap (_ split " ")
    val firstWord = flattened first()
    firstWord shouldBe "hello"
  }
}
