package day07

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 自定义UDF
  * 需求: 统计字符串的长度
  */
object UDFDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("UDFDemo")
      .setMaster("local")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // 模拟数据
    val names = List("tom","jerry","shuke")
    // def createDataFrame[A <: Product : TypeTag](data: Seq[A]): DataFrame
    val namesDF: DataFrame = spark.createDataFrame(names.map(Person))
    namesDF.createTempView("t_person")

    // 注册自定义udf
    spark.udf.register("strlen",(name:String)=>name.length)

    // 调用udf进行查询
    val res: DataFrame = spark.sql("select name as names,strlen(name) as namelength from t_person")

    // 使用别名无法改变DataFrame或DataSet具体的字段名

    res.show()
    spark.stop()
  }
}

case class Person(name:String)