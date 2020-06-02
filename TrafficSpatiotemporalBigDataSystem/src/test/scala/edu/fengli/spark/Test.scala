package edu.fengli.spark

object Test {
  def main(args: Array[String]): Unit = {
    val s = "1727#832731824213"
    val list = List("*", "#", "^", "&")
    println(contains(s, list))
  }

  @Override
  def contains(str: String, s: List[Any]): Boolean = {
    var flag = false
    for (x <- s) {
      flag = str.indexOf(x) > -1
      if (flag) {
        return flag
      }
    }
    flag
  }
}
