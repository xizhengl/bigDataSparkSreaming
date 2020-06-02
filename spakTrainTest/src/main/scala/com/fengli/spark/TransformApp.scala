package com.fengli.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 黑名单过滤
 */
object TransformApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("TransformApp")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(2))

    /**
     * 构建黑名单
     */
    val blacks = List("zs","ls")
    val blacksRDD = ssc.sparkContext.parallelize(blacks).map(x => (x ,true))

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.10.133", 6789)

    val clickLog = lines.map(x => (x.split(",")(1), x)).transform(rdd => {
      rdd.leftOuterJoin(blacksRDD)
        .filter(x => !x._2._2.getOrElse(false))
        .map(x => (x._2._1))
    })

    clickLog.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
