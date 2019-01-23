package day07

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 用UDAF实现单词统计
  */
object UDAFDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("UDFDemo")
      .setMaster("local")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // mock data
    val names = Array("dazhao","yadong","dazhao","yadong","xiaodong","dazhao")
    val namesDF = spark.createDataFrame(names.map(Person))

    // 注册临时表
    namesDF.createTempView("t_person")

    // 注册UDAF
    spark.udf.register("wc",new PersonWCUDAF)

    // 开始查询
    val res = spark.sql("select name,wc(name) from t_person group by name")
    res.show()

    spark.stop()
  }
}
