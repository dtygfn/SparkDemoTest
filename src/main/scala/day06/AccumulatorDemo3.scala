package day06

import org.apache.spark.{Accumulator, SparkConf, SparkContext}

object AccumulatorDemo3 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("AccumulatorDemo1")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val nums = sc.parallelize(List(1,2,3,4,5,6),2)
    //创建自定义Accumulator
    val accumulator = new AccumulatorTest()
    //注册累加器
    sc.register(accumulator,"acc")

    //切记不要调用transformation算子进行累加，这样会出现无法累加的情况
    //一般用foreach即可
//    nums.foreach(x=>accumulator.add(x))
    nums.foreach(accumulator.add)

    println(accumulator.value)
    sc.stop()
  }
}
