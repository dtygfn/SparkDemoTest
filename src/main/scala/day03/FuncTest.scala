package day03

import org.apache.spark.{SparkConf, SparkContext}

object FuncTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)
    rdd1.foreachPartition(x => println(x.reduce(_ + _)))

  }
}
