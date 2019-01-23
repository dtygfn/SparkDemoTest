package test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Test7 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test6").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("E:/input/textdata.txt")
    val value = lines.map(x=>(x,x.split(" ").length)).sortBy(_._2,false).take(1).toBuffer
    println(value)
    sc.stop()
  }

}
