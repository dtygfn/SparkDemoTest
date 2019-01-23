package day06.sparksql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * sparksql实现wordcount
  */
object SQLWordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SQLWordCount")
      .master("local")
      .getOrCreate()

    val ds: Dataset[String] = spark.read.textFile("E:/input/test.txt")
    import spark.implicits._
    val word: Dataset[String] = ds.flatMap(_.split(" "))

    word.createOrReplaceTempView("test")
    val res: DataFrame = spark.sql("select value,count(*) from test group by value order by count(*) desc")
    res.show()
    spark.stop()



  }
}
