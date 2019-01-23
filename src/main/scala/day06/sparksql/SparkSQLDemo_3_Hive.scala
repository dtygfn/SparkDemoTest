package day06.sparksql

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * 实现Hive-On-Spark
  */
object SparkSQLDemo_3_Hive {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkSQLDemo_3_Hive")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", "E://out/spark-warehouse") // 指定warehouse的目录
      .enableHiveSupport() // 启用hive支持
      .getOrCreate()

    import spark.implicits._

    // 创建hive表
    spark.sql("drop table src")
    spark.sql("create table if not exists src(key int, value string)")
    spark.sql("load data local inpath 'E:/kv1.txt' into table src")
    spark.sql("select * from src").show()
    spark.sql("select count(*) from src").show() // 统计该表的数据条数

    // 将获取的DataFrame转换为DataSet
    val df: DataFrame = spark.sql("select key, value from src where key <= 10 order by key")
    val ds: Dataset[String] = df.map {
      case Row(key: Int, value: String) => s"key: $key, value: $value"
    }
    ds.show()


    // 将 1 to 100 的数据直接生成DataSet
     val value: Range.Inclusive = 1.to(100)
     val valueDS: Dataset[Int] = spark.createDataset(value) // 或者toDS方法都可以
     valueDS.show()

    spark.stop()
  }
}

