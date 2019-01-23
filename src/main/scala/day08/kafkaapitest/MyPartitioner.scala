package day08.kafkaapitest

import kafka.producer.Partitioner
import kafka.utils.{Utils, VerifiableProperties}

/**
  * 自定义分区器
  * @param props
  */
class MyPartitioner(props: VerifiableProperties = null) extends Partitioner{
  private val random = new java.util.Random

  override def partition(key: Any, numPartitions: Int): Int = {
    Utils.abs(key.hashCode) % numPartitions
  }

}
