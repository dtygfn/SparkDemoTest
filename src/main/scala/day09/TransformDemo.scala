package day09

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

/***
  * 用transform可以操作DStream里的RDD
  */
object TransformDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("TransformDemo")
      .setMaster("local[2]")
    val ssc = new StreamingContext(conf,Milliseconds(2000))

    val dStream = ssc.socketTextStream("mini4",8888)
    val res: DStream[(String, Int)] = dStream.transform(rdd => {
      rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    })
    res.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
