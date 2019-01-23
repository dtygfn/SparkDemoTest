package day08.streamingdemo

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, Durations, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 实现一个简单的wordcount
  * 数据从netcat获取
  *
  */
object StreamingWC {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StreamingWC").setMaster("local[*]")
    val sc = new SparkContext(conf)
    // 创建streaming的上下文
    // 设置批处理的时间间隔
    // Durations(5)
    val ssc = new StreamingContext(sc,Seconds(5))

    /**
      * Creates an input stream from TCP source hostname:port. Data is received using
      * a TCP socket and the receive bytes is interpreted as UTF8 encoded `\n` delimited
      * lines.
      * hostname      Hostname to connect to for receiving data
      * port          Port to connect to for receiving data
      * storageLevel  Storage level to use for storing the received objects
      *                      (default: StorageLevel.MEMORY_AND_DISK_SER_2)
      * @see
      */

    // 获取netcat的数据
    // 这种获取数据会先将获得的数据以缓存的形式缓存到相应的缓存级别上
    val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("mini4",8888,StorageLevel.MEMORY_AND_DISK)

    // 开始分析数据
    val res: DStream[(String, Int)] = dstream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    // 打印到控制台
    res.print()

    // 开始提交任务到集群
    ssc.start()
    // 线程等待，等待处理下一批次任务
    ssc.awaitTermination()
  }
}
