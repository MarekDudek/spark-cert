package interretis.sparkcert.rdd

import interretis.sparktesting.ContextCreator
import org.apache.spark.rdd.RDD

class WordCount {

  def wordCount(lines: RDD[String]): RDD[(String, Int)] = {
    val words = lines flatMap (_ split " ")
    val occurences = words map ((_, 1)) cache()
    val wordCounts = occurences reduceByKey (_ + _)
    wordCounts
  }
}

object WordCount {

  def main(args: Array[String]): Unit = {

    val (input, output) = processArguments(args)
    val sc = ContextCreator.context(appName = "WordCount")
    val lines = sc textFile input

    val app = new WordCount
    val counts = app wordCount lines

    counts saveAsTextFile output
  }

  private def processArguments(args: Array[String]) = {

    val expected = 2
    val actual = args.length

    if (actual != expected) {
      Console.err.println(s"$expected arguments required and $actual given")
      sys exit 1
    }

    val input = args(0)
    val output = args(1)

    (input, output)
  }
}
