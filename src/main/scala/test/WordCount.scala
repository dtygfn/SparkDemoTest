package test

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordCoun").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("hdfs://mini1:8020/wc")
    println(lines.collect.toBuffer)
    val words = lines.flatMap(_.split(" "))
    val tup = words.map((_,1))
    val reduced = tup.reduceByKey(_+_)
    val sorted = reduced.sortBy(_._2,false)
    println(sorted.collect.toBuffer)
    sc.stop()

  }
}
