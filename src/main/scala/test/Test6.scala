package test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test6 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test6").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("E:/input/textdata.txt").flatMap(_.split("\n"))
    println(lines.filter(_.contains("Spark")).count())
    sc.stop()

  }
}
