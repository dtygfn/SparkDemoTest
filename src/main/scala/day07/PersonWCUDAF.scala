package day07

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * UDAF用于聚合操作的
  *
  */
class PersonWCUDAF extends UserDefinedAggregateFunction{
  // 定义输入类型
  override def inputSchema: StructType = {
    // structType所要的参数就是List或Array
    // A field inside a StructType.
    // @param name The name of this field.
    // @param dataType The data type of this field.
    // @param nullable Indicates if values of this field can be `null` values.
    //
    StructType(Array(StructField("str",StringType,true)))
  }

  // 定义缓存类型
  override def bufferSchema: StructType = {
    // 缓存中的数据是相同的key的个数
    StructType(Array(StructField("count",IntegerType,true)))
  }

  // 定义输出类型（返回值的类型）
  override def dataType: DataType = IntegerType

  // 是否是确定的，如果为true，就代表如果返回值和输入的值是相同的
  override def deterministic: Boolean = true

  // 初始化方法，可以在这个方法中初始值
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
  }

  // 局部聚合（分区内聚合）
  // buffer是指已经存在的值，input是指传入的新值
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Int](0) + 1
  }

  // 全局聚合，buffer1，buffer2代表的是各分区的聚合结果
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit ={
    buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)
  }

  // 在这个buffer中可以进行其他操作，比如给他加某个值，或者给他截取
  // 在最后聚合后对buffer做其他操作
  override def evaluate(buffer: Row): Any = buffer.getAs[Int](0)
}
