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

  private val numbers = Seq("1", "2", "3", "4", "5")

  "reduce" should "require the same return type as RDD" in { f =>
    // given
    val rdd = f.sc.parallelize(numbers)
    // when
    val result = rdd.reduce((acc, b) => (acc.toInt + b.toInt).toString)
    // then
    result shouldBe "15"
  }

  "fold" should "require the same return type as RDD" in { f =>
    // given
    val rdd = f.sc.parallelize(numbers)
    // when
    val result = rdd.fold("0")((acc, b) => (acc.toInt + b.toInt).toString)
    // then
    result shouldBe "15"
  }

  "aggregate" should "not require the same return type as RDD" in { f =>
    // given
    val rdd = f.sc.parallelize(numbers)
    // when
    val result = rdd.aggregate(0)((u, t) => u + t.toInt, (u1, u2) => u1 + u2)
    // then
    result shouldBe 15
  }

  private val file1 = "src/main/resources/file1.txt"
  private val file2 = "src/main/resources/file2.txt"

  "example with join" should "show how it works precisely" in { f =>
    // given
    val occurences1 = f.sc.textFile(file1).flatMap(l => l.split(",")).map(w => (w, 1))
    val occurences2 = f.sc.textFile(file2).flatMap(l => l.split(",")).map(w => (w, 1))
    occurences1.collect() should contain theSameElementsAs Seq(("1", 1), (" b", 1), ("c", 1), ("d", 1), ("2", 1), (" a", 1), ("b", 1), ("c", 1))
    occurences2.collect() should contain theSameElementsAs Seq(("1", 1), (" hadoopexam.com", 1), ("2", 1), (" quicktechie.com", 1))
    // when
    val innerJoin = occurences1 join occurences2
    innerJoin.collect() should contain theSameElementsAs Seq(("1", (1, 1)), ("2", (1, 1)))

    val outerJoin = occurences1 fullOuterJoin occurences2 cache()
    outerJoin.collect() should contain("b", (Some(1), None))
    outerJoin.collect() should contain(" hadoopexam.com", (None, Some(1)))

    val leftJoin = occurences1 leftOuterJoin occurences2
    val rightJoinRev = occurences2 rightOuterJoin occurences1
    val rightJoinRevSwapped = rightJoinRev.map { a => val (k, (l, r)) = a; (k, (r, l)) }
    leftJoin.collect() should contain theSameElementsAs rightJoinRevSwapped.collect()

    val rightJoin = occurences1 rightOuterJoin occurences2
    val leftJoinRev = occurences2 leftOuterJoin occurences1
    val leftJoinRevSwapped = leftJoinRev.map { a => val (k, (l, r)) = a; (k, (r, l)) }
    rightJoin.collect() should contain theSameElementsAs leftJoinRevSwapped.collect()
  }

  "groupByKey" should "be fed function of proper signature" in { f =>
    val pairs = f.sc.parallelize(Seq((1, "a"), (2, "b"), (1, "c")))
    val result = pairs.groupByKey()
    result.collect() should contain theSameElementsAs Seq((1, Seq("a", "c")), (2, Seq("b")))
  }
}
