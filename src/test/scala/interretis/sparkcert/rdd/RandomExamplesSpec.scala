package interretis.sparkcert.rdd

import interretis.sparktesting.SeparateContext
import org.apache.spark.rdd.RDD
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
    val innerJoin: RDD[(String, (Int, Int))] = occurences1 join occurences2
    innerJoin.collect() should contain theSameElementsAs Seq(("1", (1, 1)), ("2", (1, 1)))

    val outerJoin: RDD[(String, (Option[Int], Option[Int]))] = occurences1 fullOuterJoin occurences2 cache()
    outerJoin.collect() should contain("b", (Some(1), None))
    outerJoin.collect() should contain(" hadoopexam.com", (None, Some(1)))

    val leftJoin: RDD[(String, (Int, Option[Int]))] = occurences1 leftOuterJoin occurences2
    val rightJoinRev = occurences2 rightOuterJoin occurences1
    val rightJoinRevSwapped = rightJoinRev.map { a => val (k, (l, r)) = a; (k, (r, l)) }
    leftJoin.collect() should contain theSameElementsAs rightJoinRevSwapped.collect()

    val rightJoin: RDD[(String, (Option[Int], Int))] = occurences1 rightOuterJoin occurences2
    val leftJoinRev = occurences2 leftOuterJoin occurences1
    val leftJoinRevSwapped = leftJoinRev.map { a => val (k, (l, r)) = a; (k, (r, l)) }
    rightJoin.collect() should contain theSameElementsAs leftJoinRevSwapped.collect()
  }

  "groupByKey" should "return RDD of proper signature" in { f =>
    val pairs = f.sc.parallelize(Seq((1, "a"), (2, "b"), (1, "c")))
    val result: RDD[(Int, Iterable[String])] = pairs.groupByKey()
    result.collect() should contain theSameElementsAs Seq((1, Seq("a", "c")), (2, Seq("b")))
  }

  "reduceByKey" should "return RDD of proper signature" in { f =>
    val pairs = f.sc.parallelize(Seq(("a", 1), ("b", 2), ("a", 3)))
    val result: RDD[(String, Int)] = pairs.reduceByKey(_ + _)
    result.collect() should contain theSameElementsAs Seq(("a", 4), ("b", 2))
  }

  "reduceByKey" should "be possible to implement using groupByKey" in { f =>
    val pairs = f.sc.parallelize(Seq(("a", 1), ("b", 2), ("a", 3)))
    val result = pairs.groupByKey().mapValues(values => values.reduce(_ + _))
    result.collect() should contain theSameElementsAs Seq(("a", 4), ("b", 2))
  }

  "cogroup" should "work" in { f =>
    // given
    val rdd1 = f.sc.parallelize(Seq(('a', 1), ('b', 2), ('a', 3), ('c', 4)))
    val rdd2 = f.sc.parallelize(Seq(('a', 5), ('b', 6), ('a', 7), ('d', 8)))
    // when
    val outerJoin = rdd1 fullOuterJoin rdd2
    // then
    outerJoin.collect() should contain theSameElementsAs Seq(
      ('a', (Some(1), Some(5))),
      ('a', (Some(1), Some(7))),
      ('a', (Some(3), Some(5))),
      ('a', (Some(3), Some(7))),
      ('b', (Some(2), Some(6))),
      ('c', (Some(4), None)),
      ('d', (None, Some(8)))
    )
    // when
    val grouped = rdd1 cogroup rdd2
    // then
    grouped.collect() should contain theSameElementsAs Seq(
      ('a', (Seq(1, 3), Seq(5, 7))),
      ('b', (Seq(2), Seq(6))),
      ('c', (Seq(4), Nil)),
      ('d', (Nil, Seq(8)))
    )
  }

  "cogroup" should "work with three RDDs" in { f =>
    // given
    val rdd1 = f.sc.parallelize(Seq(('a', 1), ('b', 2), ('a', 3), ('c', 4)))
    val rdd2 = f.sc.parallelize(Seq(('a', 5), ('b', 6), ('a', 7), ('d', 8)))
    val rdd3 = f.sc.parallelize(Seq(('a', 9), ('e', 10), ('c', 11), ('d', 12)))
    // when
    val grouped = rdd1.cogroup(rdd1, rdd2)
    // then
    grouped.collect().foreach(println)

  }

  "mapValues" should "be painfully self-explanatory" in { f =>
    // given
    val rdd = f.sc.parallelize(Seq((1, 2), (3, 4), (3, 6)))
    // when
    val result = rdd.mapValues(_ + 1)
    // then
    result.collect() should contain theSameElementsAs Seq((1, 3), (3, 5), (3, 7))
  }

  "per key average" should "be possible to compute with reduceByKey and mapValues" in { f =>
    // given
    val pairs = f.sc.parallelize(Seq(("panda", 0), ("pink", 3), ("pirate", 3), ("panda", 1), ("pink", 4)))
    // when
    val averages = pairs
      .mapValues(d => (d, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(n => 1.0 * n._1 / n._2)
    // then
    averages.collect() should contain theSameElementsAs Seq(("panda", 0.5), ("pirate", 3.0), ("pink", 3.5))
  }

  "per key average" should "be possible to compute with combine by key" in { f =>
    // given
    val pairs = f.sc.parallelize(Seq(("panda", 0), ("pink", 3), ("pirate", 3), ("panda", 1), ("pink", 4)))
    // when
    val averages = pairs.combineByKey(
      createCombiner =
        v => v -> 1,
      mergeValue =
        (c: (Int, Int), v) => {
          val (sum, count) = c
          (sum + v) -> (count + 1)
        },
      mergeCombiners =
        (c1: (Int, Int), c2: (Int, Int)) => {
          val ((sum1, count1), (sum2, count2)) = (c1, c2)
          (sum1 + sum2) -> (count1 + count2)
        }
    ).mapValues(c => {
      val (sum, count) = c
      (sum: Float) / count
    })
    // then
    averages.collect() should contain theSameElementsAs Seq(("panda", 0.5), ("pirate", 3.0), ("pink", 3.5))
  }

  "lookup" should "return all matching values" in { f =>
    // given
    val rdd = f.sc.parallelize(Seq((1, 2), (3, 4), (3, 6)))
    // when
    val values = rdd.lookup(3)
    // then
    values shouldBe Seq(4, 6)
  }
}
