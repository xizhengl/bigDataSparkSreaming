package com.fengli.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *  使用SparkStreaming 处理文件系统的数据
 */
object FileWordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("FileWordCount")
    // 创建StreamingContext需要 SparkConf 和 batch interval
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.textFileStream("E:\\data")

    val result: DStream[(String, Int)] = lines
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
