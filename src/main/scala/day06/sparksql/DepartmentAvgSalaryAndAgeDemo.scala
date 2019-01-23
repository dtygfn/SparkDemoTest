package day06.sparksql

import org.apache.spark.sql.SparkSession

/**
  * 练习需求：
  *   统计各部门员工的平均薪资和平均年龄
  * 实现思路
  *   1、只能统计年龄在20岁以上的员工
  *   2、根据部门名称和员工性别进行分组
  *   3、统计每个部门的平均薪资和平均年龄
  */
object DepartmentAvgSalaryAndAgeDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("DepartmentAvgSalaryAndAgeDemo")
      .master("local")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    //获取数据
    val employee = spark.read.json("D:/qianfeng/spark/sparkcoursesinfo/spark/data/employee.json")
    val department = spark.read.json("D:/qianfeng/spark/sparkcoursesinfo/spark/data/department.json")

    //开始统计
    employee
      .filter("age>20")//只统计年龄在20岁以上的员工
      //需要将员工信息进行join
      .join(department,$"depId"===$"id")
      //根据部门名称和员工性别进行分组
      .groupBy(department("name"),employee("gender"))
      //进行聚合
      .agg(avg(employee("salary")),avg(employee("age")))
      //调用action算子
      .show()

    spark.stop()

  }

}
