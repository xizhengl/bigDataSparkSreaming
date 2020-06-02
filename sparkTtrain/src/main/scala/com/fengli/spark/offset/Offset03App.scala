package com.fengli.spark.offset

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * 1）创建StreamingContext
 * 2）从Kafka中获取数据  <==offset
 * 3）根据业务逻辑进行处理
 * 4）将处理逻辑写入外部存储 <==offset保存
 * 5）启动程序，等待程序终止
 */
object Offset03App {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Offset03App")
    val ssc = new StreamingContext(sparkConf, Seconds(10))


    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "192.168.10.133:9092",
      "auto.offset.reset" -> "smallest"
    )

    val topics = "test_topic".split(",").toSet
    val fromOffsets = Map[TopicAndPartition, Long]()



    /*
     TODO ... 获取偏移量
     MYSQL/Zk
     */
    val messages = if(fromOffsets.isEmpty) {
      // 从头消费
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    }else {
      //从指定偏移量进行消费


      val messageHandler = (mm:MessageAndMetadata[String,String]) => (mm.key, mm.message())
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String,String)](ssc,
        kafkaParams,fromOffsets, messageHandler)

    }

    messages.foreachRDD(rdd => {

      // 业务逻辑
      println("测试" + rdd.count())


      /**
       * offset提交
       */
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      offsetRanges.foreach(o =>{
        // TODO .. 如下信息到外部存储/
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
