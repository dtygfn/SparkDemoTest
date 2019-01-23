package day07

import org.apache.spark.sql.SparkSession

/**
  * 日期方法:
  * current_data
  * current_timestamp
  *
  * 数学方法:
  * round---保留几位小数
  * 随机方法：round
  * 字符串方法:
  * concat
  * concat_ws
  * def concat_ws(sep: String, exprs: Column*): Column
  * 需要自定分割符
  *
  */
object OtherFuncDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("OtherFuncDemo")
      .master("local")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val employee = spark.read.json("D://qianfeng/spark/sparkcoursesinfo/spark/data/employee.json")
    employee
      .select(employee("name"),current_date() as("date"),current_timestamp(),
        round(employee("salary"),2),
        concat(employee("name"),employee("age")),
        concat_ws("|",employee("name"),employee("age"))

      ).show()

  }

}
