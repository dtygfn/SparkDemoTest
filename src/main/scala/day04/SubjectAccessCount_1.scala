package day04

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 需求：求各个学科各个模块的访问量的top3
  * 实现思路：
  *   1、计算出每个学科的各个模块（url）的访问量
  *   2、按照学科进行分组
  *   3、组内排序并取topn
  */
object SubjectAccessCount_1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SubjectAccessCount_1")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    //获取数据
    val logs = sc.textFile("D:/qianfeng/spark/sparkcoursesinfo/spark/data/subjectaccess/access.txt")
    println(logs.collect.toBuffer)
    //将用户访问日志进行切分并返回url
    val url: RDD[String] = logs.map(line=>line.split("\t")(1))
    //url生成元组
    val tupUrl: RDD[(String, Int)] = url.map((_,1))
    // 获取每个学科的各个模块的访问量
    val reducedUrl: RDD[(String, Int)] = tupUrl.reduceByKey(_+_)
    // 通过上面的数据来获取学科信息
    val subjectAndUrlInfo: RDD[(String, (String, Int))] = reducedUrl.map(tup => {
      val url = tup._1
      //用户请求的url
      val count = tup._2
      val subject = new URL(url).getHost
      (subject, (url, count))
    })

    //按照学科信息进行分组
//    val grouped: RDD[(String, Iterable[(String, (String, Int))])] = subjectAndUrlInfo.groupBy(_._1)
    val grouped: RDD[(String, Iterable[(String, Int)])] = subjectAndUrlInfo.groupByKey
    //组内降序排序
    val sorted: RDD[(String, List[(String, Int)])] = grouped.mapValues(_.toList.sortBy(_._2).reverse)
    // 获取top3
    val res: RDD[(String, List[(String, Int)])] = sorted.mapValues(_.take(3))

    println(res.collect.toBuffer)

    sc.stop()

  }
}
