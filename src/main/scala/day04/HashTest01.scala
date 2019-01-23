package day04

/**
  * hash碰撞
  */
object HashTest01 {
  def main(args: Array[String]): Unit = {
    val key = "bigdata.learn.com"
    val numberPartitions = 3
    val rawMod = key.hashCode % numberPartitions
    val num = rawMod + (if(rawMod<0)  numberPartitions else 0)
    println(num)
  }
}
