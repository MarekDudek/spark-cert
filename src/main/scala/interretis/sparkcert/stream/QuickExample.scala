package interretis.sparkcert.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object QuickExample {

  private val name = "Spark Steaming Quick Example"

  def main(args: Array[String]): Unit = {
    println(name)
    val conf = new SparkConf();
    conf.setMaster("local[*]")
    conf.setAppName(name)
    val ssc = new StreamingContext(conf, Seconds(1))
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
