package day06

import org.apache.spark.{Accumulator, SparkConf, SparkContext}

object AccumulatorDemo2 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("AccumulatorDemo1")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    //设置分区数为2，为了进行分步式计算
    val nums = sc.parallelize(List(1,2,3,4,5,6),2)
    //该变量在Driver端
    val sum: Accumulator[Int] = sc.accumulator(0)
    //累加的过程在多个Executor端进行分布式计算
    nums.foreach(x=>sum += x)
    //不能使用transformation类型的算子实现累加
//    val value: RDD[Unit] = nums.map(x=>sum+=x)
    println(sum.value)
    sc.stop()
  }
}
