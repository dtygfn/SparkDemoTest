package day06

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * foreach和map都无法实现对Driver端的某个变量做分步式累加的过程
  */
object AccumulatorDemo1 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("AccumulatorDemo1")
      .setMaster("local[2]")
     val sc = new SparkContext(conf)
    //设置分区数为2，为了进行分步式计算
    val nums = sc.parallelize(List(1,2,3,4,5,6),2)
    //该变量在Driver端
    var sum = 0
    //累加的过程在多个Executor端进行分布式计算
//    nums.foreach(x=>sum += x)
    val value: RDD[Unit] = nums.map(x=>sum+=x)
    println(value.collect.toBuffer)
    sc.stop()
  }
}
