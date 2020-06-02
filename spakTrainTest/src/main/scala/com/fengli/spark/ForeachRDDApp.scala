package com.fengli.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 使用Spark Streaming 完成有状态统计并写入到MySQL数据库中
 */
object ForeachRDDApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("ForeachRDDApp")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 如果使用了带状态的算子，必须设置checkpoint 生成环境中设置在HDFS一个文件中
    ssc.checkpoint(".")

    val lines = ssc.socketTextStream("192.168.10.134", 6789)

    val result = lines.flatMap(_.split(" ")).map((_ ,1))
    val state = result.updateStateByKey[Int](updateFunction _)

    state.print()
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 把当前的数据去更新已有的或者是老的数据
   * @param currentValues
   * @param preValues
   * @return
   */
  def updateFunction(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val newCount = currentValues.sum
    val pre = preValues.getOrElse(0)

    Some(newCount + pre)
  }
}
