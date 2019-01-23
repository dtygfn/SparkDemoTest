package day06.sparksql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkSqlDemo_2_DataSet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkSqlDemo_2_DataSet")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._
    val personDF: DataFrame = spark.read.json("hdfs://mini1:8020/mydata/people")

    // DataFrame与DataSet之间可以相互转换，用as方法
    // 将现有的DataFrame装换为DataSet
    val personDS: Dataset[Person] = personDF.as[Person]

    //将原始数据构造为DataSet
    val value1: Dataset[Int] = Seq(1,2,3,4,5,6).toDS()
    val value2: Dataset[(String, Int, Int)] = Seq(("xiaodong",20,90),("dazhao",25,120)).toDS()


    personDS.show()
    value1.show()
    value2.show()


    spark.stop()

  }

}
// json解析时会将数值的数据类型解析成Long类型，在样例类中不能用Int来进行映射
// 用来类型映射，字段名称和数据中的字段名称需要一一对应
case class Person(name:String,age:Long,facevalue:Long)