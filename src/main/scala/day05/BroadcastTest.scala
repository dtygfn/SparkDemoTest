package day05

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 首先arr是在Driver端，在task每次执行过程中都会从Driver端拉取数据到Executor再进行计算,
  * 有多少task就会有多少次拉取arr的过程，如果arr的数据量较大，此时就有可能在Executor端发生oom
  *
  *
  */
object BroadcastTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BroadcastTest").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val arr = Array("hello","java","scala")
    //进行广播变量
    val broadcast: Broadcast[Array[String]] = sc.broadcast(arr)
    val lines = sc.textFile("hdfs://mini1:8020/wc")
    //获取广播变量的值进行计算
    val filtered: RDD[String] = lines.filter(broadcast.value.contains(_))
    println(filtered.collect.toBuffer)
    sc.stop()
  }
}
