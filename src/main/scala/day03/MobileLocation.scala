package day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MobileLocation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("MobileLocation")
      .setMaster("local")
    val sc = new SparkContext(conf)

    //获取用户访问信息
    val files:RDD[String] = sc.textFile("D:/qianfeng/spark/sparkcoursesinfo/spark/data/lacduration/log")
    //切分 如果使用flatMap会找成数组过大，造成内存溢出
    val splitedUserInfo: RDD[((String, String), Long)] = files.map(line => {
      val fields = line.split(",")
      val phone = fields(0) //手机号
      val time = fields(1).toLong //时间戳
      val lac = fields(2)//基站id
      val eventType = fields(3).toInt
      //如果是1则为离开基站范围，0是进入基站范围
      val time_long = if (eventType == 1) -time else time
      //为了进行join，需要将数据转成key-value的形式
      ((phone, lac), time_long)
    })
    //计算用户在基站停留的总时长
    val aggred: RDD[((String, String), Long)] = splitedUserInfo.reduceByKey(_+_)
    //为了方便和基站信息进行join，需要将数据进行重新整合
    val lacAndPhoneAndTime: RDD[(String, (String, Long))] = aggred.map(tup => {
      val phone = tup._1._1
      //手机号
      val lac = tup._1._2
      //基站id
      val time = tup._2 //用户在某个基站停留的总时长
      (lac, (phone, time))
    })

    //获取基站信息
    val lacInfo = sc.textFile("D:/qianfeng/spark/sparkcoursesinfo/spark/data/lacduration/lac_info.txt")
    val splitLacInfo: RDD[(String, (String, String))] = lacInfo.map(line => {
      val fields = line.split(",")
      val lac = fields(0)
      //基站id
      val x = fields(1)
      //经度
      val y = fields(2) //纬度
      (lac, (x, y))
    })

    //用户信息和基站信息进行join
    val joined: RDD[(String, ((String, Long), (String, String)))] = lacAndPhoneAndTime.join(splitLacInfo)

    //整合join后的数据便于运算
    val lacAndPhoneAndTimeAndXY: RDD[(String, String, Long, (String, String))] = joined.map(tup => {
      val lac = tup._1
      val phone = tup._2._1._1
      //手机号
      val time = tup._2._1._2
      //停留总时长
      val xy = tup._2._2 //经纬度
      (lac, phone, time, xy)
    })
    //为了便于统计用户在所有基站停留信息，生成用户粒度的数据
    val grouped: RDD[(String, Iterable[(String, String, Long, (String, String))])] = lacAndPhoneAndTimeAndXY.groupBy(_._2)
    //按照时长进行组内排序
//    grouped.map(x=>(x._1,x._2.toList.sortBy(_._3).reverse))//sortBy是scala中的函数不是算子

    //不能使用,是组内排序，不是全局排序
    //grouped.sortBy
    val sorted: RDD[(String, List[(String, String, Long, (String, String))])] = grouped.mapValues(_.toList.sortBy(_._3).reverse)
    // 获取top2
    val res = sorted.mapValues(_.take(2))
    println(res.collect().toBuffer)
    sc.stop()
  }
}
