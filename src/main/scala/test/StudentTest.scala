package test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 编程题
  */
object StudentTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("StudentTest")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)
    val sourceRDD = sc.textFile("E:\\input\\sparktest.txt")
    // 1. 一共有多少个大于20岁的人参加考试？
//    val count: Long = sourceRDD.map(line => {
//      val lineSplited = line.split(" ")
//      val name = lineSplited(2)
//      val age = lineSplited(3).toInt
//      (name, age)
//    }).filter(_._2 > 20).distinct().count()
//    println("大于20岁参加考试的人数是："+count)

    // 2. 语文科目的平均成绩是多少？
//    val courseRDD: RDD[Int] = sourceRDD.map(line => {
//      val lineSplited = line.split(" ")
//      val course = lineSplited(5)
//      val score = lineSplited(6).toInt
//      (course, score)
//    }).filter(_._1.equals("chinese")).map(_._2)
//    val count = courseRDD.count()
//    val courseSum = courseRDD.sum()
//    println("语文科目的平均成绩是"+(BigDecimal(courseSum) / BigDecimal(count)).formatted("%.2f"))

    // 3. 单个人平均成绩是多少？
//    val groupedRDD: RDD[(String, Iterable[Int])] = sourceRDD.map(line => {
//      val lineSplited = line.split(" ")
//      val name = lineSplited(2)
//      val score = lineSplited(6).toInt
//      (name, score)
//    }).groupByKey
//    val avgRDD: RDD[(String, String)] = groupedRDD.mapValues(it => {
//      val scoreList = it.toList
//      val count = scoreList.length
//      var sum = 0
//      for (elem <- scoreList) {
//        sum += elem
//      }
//      (BigDecimal(sum) / BigDecimal(count)).formatted("%.2f")
//    })
//    println("平均成绩是："+avgRDD.collect.toBuffer)

    // 4. 总成绩大于150分，且数学大于等于60，且年龄大于等于20岁的学生的平均成绩是多少？
    sourceRDD.map(line => {
      val lineSplited = line.split(" ")
      val name = lineSplited(2)
      val age = lineSplited(3).toInt
      val course = lineSplited(5)
      val score = lineSplited(6).toInt
      (name, age, course, score)
    }).filter(_._2>=20)
      .map(tup=>(tup._1,tup._3,tup._4))
      .groupBy(_._1)
      .filter(it=>it._2.toList(1)._3>=60).map(tup=>{
      val name = tup._1
      val tuplist = tup._2.toList
      val count = tuplist.length
      var sum = 0
      for (elem <- tuplist) {
        sum += elem._3
      }
      (name,sum,count)
    }).filter(_._2>150)
      .foreach(tup=>{
        println(tup._1+" "+ BigDecimal(tup._2)./(BigDecimal(tup._3)).formatted("%.2f"))
      })
  }

}
