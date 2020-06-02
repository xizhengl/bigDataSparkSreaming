package edu.fengli.spark

object Test2 {
  def main(args: Array[String]): Unit = {
    val n = 41.839395 + ""
    println(n.formatted("%.6").toDouble)
  }
}
