package day07

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * typed练习
  */
object TypedDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("TypedDemo")
      .master("local")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._
    val employee:DataFrame = spark.read.json("D://qianfeng/spark/sparkcoursesinfo/spark/data/employee.json")
    val employDS = employee.as[Employee]
    val employee2:DataFrame = spark.read.json("D://qianfeng/spark/sparkcoursesinfo/spark/data/employee2.json")
    val employDS2 = employee2.as[Employee]
    val department = spark.read.json("D://qianfeng/spark/sparkcoursesinfo/spark/data/department.json")
    val departmentDS = department.as[Department]
    //重新分区
    //DataFrame转RDD的方法
    println("初始分区数:"+employee.rdd.partitions.length)
    // 使用repartition进行重行分区，默认shuffle是true
    val empRep = employee.repartition(8)
    //重新分区coalesce的默认shuffle是false
    val empCoa = employee.coalesce(7)

    println("repartition:"+empRep.rdd.partitions.length)
    println("colaesce:"+empCoa.rdd.partitions.length)


    //去重
    // distinct的返回值也是DataSet，是按照整条数据完整比对进行去重
    employee.distinct().show()
    // dropDuplicates,返回值是DataSet，是按照某指定的某一个或多个字段进行比对进行去重
    // def dropDuplicates(colNames: Seq[String]): Dataset[T]
    employee.dropDuplicates(Array("name")).show()



    // except，filter
    // def except(other: Dataset[T]): Dataset[T]
    // except获取当前DataSet中有但有另一个DataSet中没有的元素，想当与差集
    // filter需要给一个过滤逻辑，如果返回true，则保留元素，否则过滤掉
    employDS.except(employDS2).show()
    employDS.filter(emp=>emp.age >30).show()
    // 并集
    employDS.union(employDS2).show()
    // 交集
    employDS.intersect(employDS2).show()


    // joinWith 将两个数据集join到一起，需要指定join字段
    // sort需要指定字段进行排序
    employDS.joinWith(departmentDS,$"depId"===$"id").show()
    employDS.sort($"age".desc).show()


    // collect_set 将指定字段的值都收集到一起，会按照这个字段进行去重
    // collect_list 同上，但不会按照字段进行去重
    employDS
      .groupBy(employDS("depId"))
      .agg(collect_set(employDS("name")),collect_list(employDS("name")))
      .show()

    // avg,sum,max,min,count,countDistinct
    employee
        .join(department,$"depId"===$"id")
        .groupBy(department("name"))
        .agg(avg(employee("salary")), sum(employee("salary")),
          max(employee("salary")), min(employee("salary")),
          count(employee("name")),countDistinct(employee("name"))
        ).show()


    // untyped: select where join groupBy agg

    spark.stop()
  }
}

case class Employee(name:String,age:Long,depId:Long,gender:String,salary:Double)

case class Department(id:Long,name:String)
