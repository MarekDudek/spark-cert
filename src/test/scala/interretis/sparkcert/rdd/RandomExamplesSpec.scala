package interretis.sparkcert.rdd

import interretis.sparktesting.SeparateContext
import org.scalatest.Matchers

class RandomExamplesSpec extends SeparateContext with Matchers {

  "example with flatMap" should "work" in { f =>
    // given
    val lines = f.sc.parallelize(Array("hello we are learning Spark from someplace", "We are also learning Hadoop from someplace"))
    // when
    val flattened = lines flatMap (_ split " ")
    val firstWord = flattened first()
    // then
    firstWord shouldBe "hello"
  }

  "example with union" should "work" in { f =>
    // given
    val words1 = Seq("An", "Apple", "a", "Day", "Keep", "Doctor", "Away")
    val words2 = Seq("An", "Apple", "a", "Day")
    val rdd1 = f.sc.parallelize(words1)
    val rdd2 = f.sc.parallelize(words2)
    // when
    val union = rdd1 union rdd2
    // then
    union.collect() should contain theSameElementsAs words1 ++ words2
  }
}
