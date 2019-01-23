package day05

/**
  * 实现自定义排序
  */

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CustomSortDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("CustomSortDemo")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    val girlInfo = sc.parallelize(List(("mimi",99,33),("bingbing",80,35),("yuanyuan",80,32)))

    //第一种排序
    //Girl(goddes._2,goddes._3) 仅仅是指定了比较规则
//    import MyPredef.girlOrding
//    val res: RDD[(String, Int, Int)] = girlInfo.sortBy(goddes=>Girl(goddes._2,goddes._3),false)


    val res: RDD[(String, Int, Int)] = girlInfo.sortBy(girl=>Girl(girl._2,girl._3),false)
    println(res.collect.toBuffer)
    sc.stop()
  }

}

//第一种排序
//case class Girl(fv:Int,age:Int)

//第二种排序方式
case class Girl(fv:Int,age:Int) extends Ordered[Girl]{
  override def compare(that: Girl): Int = {
    if (this.fv != that.fv)
      this.fv - that.fv
    else
      that.age - this.age
  }
}