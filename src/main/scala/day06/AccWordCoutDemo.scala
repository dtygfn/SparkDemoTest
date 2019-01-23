package day06

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

class AccWordCoutDemo  extends AccumulatorV2[String,mutable.HashMap[String,Int]] {
  //初始值
  private  val hashAcc = new mutable.HashMap[String,Int]()

  //检查初始值是否为空
  override def isZero: Boolean = hashAcc.isEmpty

  //copy一个新的累加器
  override def copy(): AccumulatorV2[String,mutable.HashMap[String,Int]] ={
    val newAcc = new AccWordCoutDemo
    //有可能多个task同时往初始值中写值，有可能出现线程安全，此时最好加锁
    newAcc.synchronized(
      newAcc.hashAcc ++= hashAcc
    )
    newAcc
  }

  //重置累加器
  override def reset(): Unit = hashAcc.clear()

  //局部累加
  override def add(v: String): Unit = {
    hashAcc.get(v) match {
      case None => hashAcc += ((v,1))
      case Some(x) => hashAcc += ((v,x+1))
    }
  }

 //全局合并
 override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
   other match {
     case o: AccumulatorV2[String, mutable.HashMap[String, Int]] => {
       for ((k, v) <- o.value) {
         hashAcc.get(k) match {
           case None => hashAcc += ((k, v))
           case Some(x) => hashAcc += ((k, x + v))
         }
       }
     }
   }
 }

  override def value: mutable.HashMap[String,Int] =hashAcc
}


object AccWordCoutDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("AccWordWordDemo")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List("a","c","a","d","a","c"),2)

    //调用累加器对象
    val acc = new AccWordCoutDemo

    //注册累加器
    sc.register(acc,"accWc")
    //开始分步式累加
    rdd.foreach(acc.add)

    println(acc.value)

    sc.stop()
  }
}