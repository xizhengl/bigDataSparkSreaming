package com.fengli.spark.offset

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}


/**
 * 当前方法可以避免当Spark Streaming作业意外停止时的数据丢失，但是会造成数据重复！
 * 当Spark Streaming作业意外停止后再次重启程序会从最初偏移量开始重复消费数据（最早生产出的数据开始）
 */
object Offset01App {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Offset01App")
    val ssc = new StreamingContext(sparkConf, Seconds(10))


    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "192.168.10.133:9092",
      "auto.offset.reset" -> "smallest"
    )

    val topics = "test_topic".split(",").toSet
    val messages: InputDStream[(String, String)] = KafkaUtils
        .createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    messages.foreachRDD(rdd => {
      if(!rdd.isEmpty()){
        println("测试：" + rdd.count())
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
