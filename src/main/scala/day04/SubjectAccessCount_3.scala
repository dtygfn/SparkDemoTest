package day04

import java.net.URL

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * 优化：实现自定义分区，将不同的学科信息放在不同的文件
  * 需求：求各个学科各个模块的访问量的top3
  * 实现思路：
  *   1、计算出每个学科的各个模块(url)的访问量
  *   2、按照学科进行分组
  *   3、组内排序并获取topn
  */
object SubjectAccessCount_3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SubjectAccessCount")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    //获取数据
    val logs = sc.textFile("D:/qianfeng/spark/sparkcoursesinfo/spark/data/subjectaccess/access.txt")

    //将用户访问日志进行切分
    val url: RDD[String] = logs.map(line=>line.split("\t")(1))

    //将url生成元组便于聚合
    val tupurl: RDD[(String, Int)] = url.map((_,1))
    //获取每个学科的各个模块的访问量
    val reducedUrl: RDD[(String, Int)] = tupurl.reduceByKey(_+_).cache

    //通过上面的数据来获取学科信息
    val subjectAndUrlInfo: RDD[(String, (String, Int))] = reducedUrl.map(tup => {
      val url = tup._1 //用户请求的url
      val count = tup._2 //url对应的pv
      val subject = new URL(url).getHost
      (subject, (url, count))
    }).cache()//在一个job中可以定义多个缓存

//    val partitioned: RDD[(String, (String, Int))] = subjectAndUrlInfo.partitionBy(new HashPartitioner(3))
//
//    partitioned.saveAsTextFile("E:/output/2019-01-14")

    //获取学科信息
    val subjects: Array[String] = subjectAndUrlInfo.keys.distinct().collect()
    //调用自定义分区器
    val partitioner = new SubjectPartitioner(subjects)

    //开始进行分区
    val partitioned = subjectAndUrlInfo.partitionBy(partitioner)
    val res: RDD[(String, (String, Int))] = partitioned.mapPartitions(it => {
      it.toList.sortBy(_._2._2).reverse.take(3).iterator
    })
    res.saveAsTextFile("E:/output/2019-01-14-1")
    sc.stop()
  }
}

/***
  * 自定义分区器
  * @param subjects
  */
class SubjectPartitioner (subjects: Array[String]) extends Partitioner{
  //用来存储学科对应的分区号
  val subjectAndPartition = new mutable.HashMap[String,Int]()
  //计数器
  var i = 0
  for (subject<-subjects){
    subjectAndPartition +=(subject->i)
    i += 1
  }

  /**
    * 获取分区数
    * @return
    */
  override def numPartitions: Int = subjects.length

  /***
    * 获取分区号
    * @param key
    * @return
    */
  override def getPartition(key: Any): Int = subjectAndPartition.getOrElse(key.toString,0)
}