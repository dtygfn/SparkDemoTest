package day08.kafkaapitest

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

/**
  * 实现一个生产者
  * 模拟一些数据不断的发送到kafka的topic
  * 实现自定义分区器
  */
object ProducerDemo {
  def main(args: Array[String]): Unit = {
    // 配置kafka的配置信息
    val props = new Properties()
    // 配置序列化类型
    props.put("serializer.class","kafka.serializer.StringEncoder")
    // 指定kafka列表
    props.put("metadata.broker.list","mini4:9092,mini5:9092,mini6:9092")
    // 设置发送数据的响应方式 0,1,-1
    props.put("request.required.acks","1")
    // 设置分区器
//    props.put("partitioner.class","kafka.producer.DefaultPartitioner")
    props.put("partitioner.class","day08.kafkaapitest.MyPartitioner")


    // 指定分区器
    val topic = "test1"

    // 创建producer的配置对象
    val config = new ProducerConfig(props)

    // 创建producer对象
    val producer: Producer[String, String] = new Producer(config)

    // 模拟数据
    for (i <- 1 to 100){
      val msg:String = s"$i: Producer send data"
      producer.send(new KeyedMessage[String,String](topic,msg))
      Thread.sleep(500)
    }

    //一次性发送，之后才能释放资源，若是想要多次发送就不能关闭
    producer.close()
  }
}
