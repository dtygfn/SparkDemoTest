package day09

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object LoadTopicDataWC {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
        .setAppName("")
        .setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(2))

    // 设置检查点
    ssc.checkpoint("hdfs://mini4:8020/cp-2019-01-10-0")

    // 请求kafka的配置信息
    val Array(zkQuorum, group, topics, numThread) = args

    // 将topic封装到map中
    val topicMap: Map[String, Int] = topics.split(",").map((_, numThread.toInt)).toMap

    // 开始调用kafka工具类获取topic信息
    val dstream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,zkQuorum,group,topicMap,MEMORY_AND_DISK)

    // 因为获取的kafka的数据是key，value的形式，其中key为偏移量，在实际统计中不需要，可以过滤掉

    val lines: DStream[String] = dstream.map(_._2)

    // 开始统计
    val tups: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1))
    val res: DStream[(String, Int)] = tups.updateStateByKey(func,new HashPartitioner(ssc.sparkContext.defaultParallelism),true)

    res.print

    ssc.start()
    ssc.awaitTermination()


  }

  val func = (it:Iterator[(String, Seq[Int], Option[Int])]) =>{
    it.map(x=>{
      (x._1,x._2.sum+x._3.getOrElse(0))
    })
  }

}


