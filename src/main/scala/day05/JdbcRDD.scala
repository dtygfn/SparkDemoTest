package day05

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object JdbcRDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("jdbcRDD").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val user = "root"
    val pwd = "123456"
    val url = "jdbc:mysql://mini1:3306/test?useUnicode=true&characterEncoding=utf8"
    val conn =()=>{//获得连接的函数
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection(url,user,pwd)

    }
    val sql = "select * from tohdfs where id >=? and id<=?"
    val res: JdbcRDD[(Int, String)] = new JdbcRDD(
      sc, conn, sql, 1, 8, 1,
      res => {
        val id = res.getInt("id")
        val name = res.getString("name")
        (id, name)
      }
    )

    println(res.collect.toBuffer)
    sc.stop()
  }
}
