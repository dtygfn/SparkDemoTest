package day06

import org.apache.spark.util.AccumulatorV2

/**
  * 在继承AccumulatorV2的时候需要实现泛型，给定输入和输出的类型
  * 然后需要再重写几个方法
  */
class AccumulatorTest extends AccumulatorV2[Int,Int]{
  //先初始化一个输出值的变量
  var sum:Int = _

  //判断初始值是否为空
  override def isZero: Boolean = sum == 0

  //copy一个新的累加器
  override def copy(): AccumulatorV2[Int, Int] = {
    //需要创建当前自定义累加对象
    val acc = new AccumulatorTest
    //需要将当前数据拷贝到新的累加器数据里面
    //也就是将原有的累加器数据中copy到当前的新的累加器数据中
    acc.sum = this.sum
    acc
  }

  //重置一个累加器，将累加器中的数据初始化
  override def reset(): Unit = sum = 0

  //给定具体累加的过程，属于每一个分区进行累加的方法(局部累加)
  override def add(v: Int): Unit = {
    //v就是该分区中的某个元素
    sum += v
  }

  //全局累加,合并每一分区的累加值
  override def merge(other: AccumulatorV2[Int, Int]): Unit = sum += other.value

  //输出值
  override def value: Int = sum
}
