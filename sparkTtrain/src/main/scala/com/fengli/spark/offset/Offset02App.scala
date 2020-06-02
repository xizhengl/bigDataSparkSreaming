package com.fengli.spark.offset

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}



/**
 * 采用checkpoint的方式可以解决数据丢失、数据重复的问题。
 * 但是如果使用checkpoint的方式的话，一旦修改了Spark Streaming作业中的业务逻辑那就checkpoint将会失效
 */
object Offset02App {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Offset02App")

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "192.168.10.133:9092",
      "auto.offset.reset" -> "smallest"
    )
    val topics = "test_topic".split(",").toSet

    val checkpointDirectory = "hdfs://192.168.10.133:8020/offset"

    // Function to create and setup a new StreamingContext
    def functionToCreateContext(): StreamingContext = {
      val ssc = new StreamingContext(sparkConf, Seconds(10))   // new context

      val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

      // 设置checkpoint
      ssc.checkpoint(checkpointDirectory)   // set checkpoint directory
      messages.checkpoint(Duration(10*1000))

      messages.foreachRDD(rdd => {
        if(!rdd.isEmpty()){
          println("测试：" + rdd.count())
        }
      })

      ssc
    }


    val ssc = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext _)

    ssc.start()
    ssc.awaitTermination()
  }
}
