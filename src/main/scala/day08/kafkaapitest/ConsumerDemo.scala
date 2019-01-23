package day08.kafkaapitest

import java.util.Properties
import java.util.concurrent.Executors

import kafka.consumer.{Consumer, ConsumerConfig, KafkaStream}

import scala.collection.mutable

class ConsumerDemo(val consumer:String,val stream:KafkaStream[Array[Byte], Array[Byte]]) extends Runnable{
  override def run(): Unit = {
    val it = stream.iterator()
    while (it.hasNext()){
      val data = it.next()
      val topic = data.topic
      val offset = data.offset
      val partition = data.partition
      val msgByte: Array[Byte] = data.message()
      val msg:String = new String(msgByte)
      println(s"consumer: $consumer,topic:$topic,offset:$offset,partition:$partition,msg:$msg")
    }
  }
}
object ConsumerDemo{
  def main(args: Array[String]): Unit = {
    // 定义获取topic
    val topic = "test1"

    // 定义map，用于存储多个topic的信息,key=topic，value=获取topic的线程数(consumer的数量)
    val topics = new mutable.HashMap[String,Int]()
    topics.put(topic,1)
    // 配置
    val props = new Properties()
    // 定义group
    props.put("group.id","group1")
    // 指定zookeerper列表
    props.put("zookeeper.connect","mini4:2181,mini5:2181,mini6:2181")
    // 指定偏移量
    // 如果zookeerper没有offset值或offset值超出范围，那么就给个初始的offset
    props.put("auto.offset.reset","smallest")

    // 创建配置类，封装配置信息
    val config = new ConsumerConfig(props)

    // 创建consumer对象
    val consumer = Consumer.create(config)

    // 开始消费数据，map中 key=topic，value=topic对应的数据
    val streams: collection.Map[String, List[KafkaStream[Array[Byte], Array[Byte]]]] = consumer.createMessageStreams(topics)

    // 将topic中的数据拿出来，kafkaStream中，key=offset，value=data
    val topicStreams: Option[List[KafkaStream[Array[Byte], Array[Byte]]]] = streams.get(topic)

    // 创建固定线程池
    val pool = Executors.newFixedThreadPool(3)

    for(i<-0 until topicStreams.size){
      // submit有返回值，execute方法无返回值
      pool.execute(new ConsumerDemo(s"consumer:$i",topicStreams.get(i)))
    }
  }
}
