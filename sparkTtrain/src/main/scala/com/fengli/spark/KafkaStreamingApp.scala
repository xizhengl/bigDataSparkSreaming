package com.fengli.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * SparkStreaming 对接Kafka
 */
object KafkaStreamingApp {
  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      System.err.println("Usage: KafkaReceiverWordCount <zkQuorum> <group> <topics> <numThreads>")
    }
    val Array(zkQuorum, group, topics, numThreads) = args

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaStreamingApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topicsMap = topics.split(",").map((_, numThreads.toInt)).toMap

    val messages = KafkaUtils.createStream(ssc,zkQuorum,group,topicsMap)

    messages.map(_._2).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
