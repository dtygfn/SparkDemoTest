package day06

import org.apache.spark.util.{DoubleAccumulator, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 累加器为我们实现了一些简单的累加器
  */
object AccumulatorDemo4 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("AccumulatorDemo1")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    val nums1 = sc.parallelize(List(1,2,3,4,5,6),2)
    val nums2 = sc.parallelize(List(1.1,2.2,3.3,4.4,5.5,6.6),2)


    //创建并注册累加器
    def longAccumulator(name:String):LongAccumulator={
      val acc = new LongAccumulator
      sc.register(acc,name)
      acc
    }

    val acc1 = longAccumulator("longAccumulator")
    nums1.foreach(x=>acc1.add(x))

    def doubleAccumulator(name:String):DoubleAccumulator={
      val acc = new DoubleAccumulator
      sc.register(acc,name)
      acc
    }

    val acc2 = doubleAccumulator("doubleAccumulator")
    nums2.foreach(x=>acc2.add(x))
    println(acc1.value)
    println(acc2.value)
    sc.stop()
  }
}
