package com.fengli.spark.project.spark

import com.fengli.spark.project.dao.{CourseClickCountDAO, CourseSearchClickCountDAO}
import com.fengli.spark.project.domain.{ClickLog, CourseClickCount, CourseSearchClickCount}
import com.fengli.spark.project.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
 * 使用SparkStreaming处理Kafka过来的数据
 */
object StreamingApp {
  def main(args: Array[String]): Unit = {

    if (args.length != 4){
      println("Usage: StreamingApp <zkQuorum> <groupId> <topicMap> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum , groupId , topics, numThreads) = args

    val sparkConf = new SparkConf() // .setMaster("local[5]").setAppName("StreamingApp")

    val ssc = new StreamingContext(sparkConf, Seconds(60))

    val topicMap = topics.split(" ").map((_, numThreads.toInt)).toMap

    val lines: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap)

//    lines.map(_._2).count().print()

    val logs = lines.map(_._2)
    val cleanData = logs.map(line => {
      val infos: Array[String] = line.split("\t")
      val url = infos(2).split(" ")(1)
      var courseId = 0

      // 课程编号
      if (url.startsWith("/class")) {
        val courseIdHTHML = url.split("/")(2)
        courseId = courseIdHTHML.substring(0, courseIdHTHML.lastIndexOf(".")).toInt
      }

      ClickLog(infos(0), DateUtils.parseTOMinute(infos(1)), courseId, infos(3), infos(4).toInt)
    }).filter(clickLog => clickLog.courseId != 0)


    //    cleanData.print()

    //功能1 统计今天到现在为止实战课程的访问量

    cleanData.map(x => {
      (x.time.substring(0, 8) + "_" + x.courseId, 1)
    }).reduceByKey(_ + _).foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        val list = new ListBuffer[CourseClickCount]

        partitionRecords.foreach(pair => {
          list.append(CourseClickCount(pair._1, pair._2))
        })

        CourseClickCountDAO.save(list)
      })
    })



    // 功能2
    cleanData.map(x => {
      // http://search.yahoo.com/search?p=Spark SQL实战
      val referer = x.referer.replaceAll("//", "/")
      val splits = referer.split("/")

      var host = ""
      if (splits.length > 2){
        host = splits(1)
      }

      (host, x.courseId, x.time)
    }).filter(_._1 != "").map(x => {
      (x._3.substring(0,8) + "_" + x._1 + "_" + x._2 , 1)
    }).reduceByKey(_ + _).foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        val list = new ListBuffer[CourseSearchClickCount]

        partitionRecords.foreach(pair => {
          list.append(CourseSearchClickCount(pair._1, pair._2))
        })

        CourseSearchClickCountDAO.save(list)
      })
    })


    ssc.start()
    ssc.awaitTermination()
  }
}
