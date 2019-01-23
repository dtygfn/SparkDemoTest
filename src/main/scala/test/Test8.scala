package test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object Test8 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("phoneproject").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //从磁盘上读取数据
    val lines = sc.textFile("E:/input/test8.txt")
    val maped: RDD[(String, (String, String))] = lines.map(line => {
      val fields: Array[String] = line.split("\t")
      val id = fields(0)
      val time = fields(1)
      val url = fields(2)
      (id, (time, url))
    })

    //分组
    val grouped: RDD[(String, Iterable[(String, String)])] = maped.groupByKey
    //组内排序并去top2
    val sorted: RDD[(String, List[(String, String)])] = grouped.mapValues(_.toList.sortBy(_._1).take(2))
    //在输出到磁盘之前格式化输出格式
    val res: RDD[String] = sorted.flatMap(tup => {
      val id = tup._1
      val listBuffer = new ListBuffer[String]
      for (list <- tup._2) {
        listBuffer += id + "\t" + list._1 + "\t" + list._2
      }
      listBuffer
    })
    //输出到磁盘上
    res.saveAsTextFile("E:/out/2019-01-05-2")

  }

}
