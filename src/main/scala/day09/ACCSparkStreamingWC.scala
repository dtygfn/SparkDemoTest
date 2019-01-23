package day09

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 在历史结果应用到当前批次处理的需求时，可以使用updateStateByKey原语实现
  * 实现历史批次累加功能也可以借助数据库来实现
  * updateStateByKey只有获取历史批次结果应用到当前批次中的功能，该原语没有存储历史批次结果功能
  * 所以，实现批次累加必须进行checkpoint--->streaming中，checkpoint具有存储历史数据的功能
  */
object ACCSparkStreamingWC {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ACCSparkStreamingWC").setMaster("local[2]")

    val ssc = new StreamingContext(conf,Seconds(5))

    ssc.checkpoint("hdfs://mini4:8020/cp-20180110-2")
    // 获取数据
    val dStream = ssc.socketTextStream("mini4",8888)

    // 开始统计
    val tups: DStream[(String, Int)] = dStream.flatMap(_.split(" ").map((_,1)))
    // 调用updateStateByKey原语进行批次累加
    /**
      * def updateStateByKey[S: ClassTag](
      * updateFunc: (Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)],
      * partitioner: Partitioner,
      * rememberPartitioner: Boolean,
      * initialRDD: RDD[(K, S)]): DStream[(K, S)] = ssc.withScope
      */
    // 用来实现
    val res: DStream[(String, Int)] = tups.updateStateByKey(func,new HashPartitioner(ssc.sparkContext.defaultParallelism),true)
    res.print()

    ssc.start()
    ssc.awaitTermination()

  }

  /**
    * 迭代器中
    * 第一个参数：数据中的key
    * 第二个参数：当前批次中相同key对应的value Seq(1,1,1)
    * 第三个参数：历史结果中相同的key对用的value Some(2)
    */
  val func = (it:Iterator[(String, Seq[Int], Option[Int])]) =>{
    it.map(x=>{
      (x._1,x._2.sum+x._3.getOrElse(0))
    })
  }
}
