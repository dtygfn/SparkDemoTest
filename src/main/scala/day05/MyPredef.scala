package day05

object MyPredef {
  //第一种排序
  implicit val girlOrding = new Ordering[Girl]{
    override def compare(x: Girl, y: Girl): Int = {
      if (x.fv != y.fv)
        x.fv - y.fv
      else
        y.age- x.age

    }
  }
}
