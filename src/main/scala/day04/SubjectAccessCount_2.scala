package day04

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * 优化：加入缓存的使用
  * 需求：求各个学科各个模块的访问量的top3
  * 实现思路：
  *   1、计算出每个学科的各个模块(url)的访问量
  *   2、按照学科进行分组
  *   3、组内排序并获取topn
  */
object SubjectAccessCount_2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SubjectAccessCount")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    //获取数据
    val logs = sc.textFile("D:/qianfeng/spark/sparkcoursesinfo/spark/data/subjectaccess/access.txt")

    //学科信息
    val subjects = Array("http://java.learn.com","http://ui.learn.com","http://bigdata.learn.com/","http://android.learn.com","http://h5.learn.com")

    //将用户访问日志进行切分
    val url: RDD[String] = logs.map(line=>line.split("\t")(1))

    //将url生成元组便于聚合
    val tupurl: RDD[(String, Int)] = url.map((_,1))
    //获取每个学科的各个模块的访问量
    //在cache的时候一定要考虑一个问题，cache的数据不能太大，这个缓存的数据和节点的内存量有关系
    val reducedUrl: RDD[(String, Int)] = tupurl.reduceByKey(_+_).cache

    //用for循环对学科和数据进行匹配
    for(subject<- subjects){
      val filteredSubject: RDD[(String, Int)] = reducedUrl.filter(_._1.startsWith(subject))
      val res: Array[(String, Int)] = filteredSubject.sortBy(_._2,false).take(3)
      println(res.toBuffer)
    }
    sc.stop()
  }
}
