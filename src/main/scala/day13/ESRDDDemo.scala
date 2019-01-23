package day13

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
object ESRDDDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    // 设置es配置信息
    conf.setAppName("ESRDDDemo")
      .setMaster("local")
      .set("es.nodes","mini4,mini5,mini6")
      .set("es.port","9200")
      .set("es.index.auto.create","true")// 当插入数据时没有index则创建
    val sc = new SparkContext(conf)
    val query =
      """
        {"query":{"match_all":{}}}
      """.stripMargin

    // 输出类型中key相当于每条数据对应的id值
    // value就是id对应的数据
    // Map中的key=字段的名称
    val queryRDD: RDD[(String, collection.Map[String, AnyRef])] = sc.esRDD("blog",query)

    // 将id对应的数据拿到
    val valueRDD: RDD[collection.Map[String, AnyRef]] = queryRDD.map(_._2)

    // 获取每个字段的数据
    val dataRDD: RDD[(AnyRef, AnyRef, AnyRef)] = valueRDD.map(line => {
      val id: AnyRef = line.getOrElse("id", "")
      val title: AnyRef = line.getOrElse("title", "")
      val context = line.getOrElse("content", "")
      (id, title, context)
    })

    println(dataRDD.collect().toBuffer)

    sc.stop()
  }

}
