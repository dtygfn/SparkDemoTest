package day06.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSqlDemo_1 {
  def main(args: Array[String]): Unit = {
    //模板方法
    val spark: SparkSession = SparkSession
      .builder()//用来构建的方法
      .appName("SparkSqlDemo_1")
      .master("local[2]")
      .getOrCreate() // 配置完信息最后加该方法
    /**
      * DSL语句风格
      */
    //DataFrame是DataSet的弱类型
    val df: DataFrame = spark.read.json("D:/qianfeng/spark/sparkcoursesinfo/spark/data/people.json")

//    df.select("name").show()
//    df.select("age").show()
//    df.show()

    import spark.implicits._
    // 使用$就相当于引用变量的值，可以再进行计算
    // df.select($"name",$"age" + 1,$"facevalue").show()  //select用来获取数据

    // df.select("name","age").where("age>20").show() //条件操作
    // df.select($"name",$"age").where($"age">20).show()
    // df.filter($"age">20).show() // 过滤，属于条件操作

//    df.groupBy("age").count().show() // 求同一年龄的人数

    /**
      * sql语句风格
      */
    // 用来生成临时表，便于sql语句进行操作
    df.createTempView("t_person")
//    //调用上下文中的sql方法进行查询
    val sqlDF: DataFrame = spark.sql("select * from t_person")
//    sqlDF.show()
//
//    println(sqlDF.schema)
////    df.printSchema()
    sqlDF.write.mode("append").json("E://out/spark-sql-out")

    spark.stop()
  }
}
