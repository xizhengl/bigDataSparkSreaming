package edu.fengli.spark

import java.util

import _root_.kafka.serializer.StringDecoder
import edu.fengli.utils.{HBaseUtils, LocationUtils}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming对接 Kafka采用 Direct处理
 * @author lixizheng
  */
object KafkaDirectApp {

  def main(args: Array[String]): Unit = {

    if(args.length != 2) {
      System.err.println("Usage: KafkaDirectApp <brokers> <topics>")
      System.exit(1)
    }
    val Array(brokers, topics) = args

    // 基础配置
    val sparkConf = new SparkConf().setAppName("KafkaDirectApp").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 外部传入参数
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String,String]("metadata.broker.list"-> brokers)


    // Spark Streaming对接Kafka
    val messages = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
    ssc,kafkaParams,topicsSet
    )

    // 清洗数据
    val cleanData: DStream[Any] = messages.map(_._2)
      .map(x => {
        val strings = x.split("\t")

        def contains(str: String, s: List[String]): Boolean = {
          var flag = false
          for (x <- s) {
            flag = str.indexOf(x) > -1
            if (flag) {
              return flag
            }
          }
          flag
        }
        val list: List[String] = List("#", "*", "^")
        // 判断是否有脏数据
        val bool = contains(strings(0), list)
        if (!bool) {
          if (strings(0) != "" && strings(1) != "" && strings(2) != "" && strings(3) != "") {
            (strings(0), strings(1), strings(2) + "-" + strings(3))
          }else{
            ""
          }
        }
      }).filter(_ != "")


    // 在HBase中创建用户表
    val utils = HBaseUtils.getInstance()
    utils.createTable("userInfo", "info")
    import scala.collection.JavaConverters._
    val list = List("metro", "bus").asJava
    // TODO 建表操作
    utils.createTable("residesCount",list)

    cleanData.foreachRDD(rdd =>
      rdd.foreachPartition { partitionOfRecords =>
        val hBaseUtils = HBaseUtils.getInstance()
        partitionOfRecords.foreach(pair => {
          var line = pair.toString
          line = pair.toString.substring(1, line.length - 1)
          val strings = line.split(",")
          if (strings.length == 3 && strings(0) != null) {
            val uid = strings(0)
            val time = strings(1)
            val lacCell = strings(2)

            println(uid, time, lacCell)

            // TODO 查询用户当前经纬度信息
            val LongLat = hBaseUtils
              .findByRowKeyGetColumnValue("baseLongLatData", lacCell, "info", "longLat")

            if (LongLat != null) {
              val longLats = LongLat.split("-")
              // TODO 用户位置位置信息入库
              hBaseUtils.put("userInfo", uid, "info", "LongLat", LongLat)

              // 获取当前经纬度
              val currentLong = longLats(0).toDouble
              val currentLat = longLats(1).toDouble

              // TODO 查询所有静态位置信息
              val list: util.List[String] = hBaseUtils
                .getTableAllRowKeyAndValue("tripStaticData", "info", "modeName")
              val toList: List[String] = list.asScala.toList

              var min = Double.MaxValue
              // 最终存储出行方式
              var endModeName = ""
              // 最终经纬度
              var endLongLat = ""
              for (x <- toList){
                val results = x.split("-")
                if (results.length == 3) {
                  // 纬度
                  val Lat = results(1).toDouble
                  // 经度
                  val Long = results(0).toDouble
                  // 静态地点
                  val modeName = results(2)
                  // 计算距离
                  val distance: Double = LocationUtils.getInstance()
                    .getDistance(currentLat, currentLong, Lat, Long)
                  // 计算最短距离
                  if (distance < min){
                    min = distance
                    endModeName = modeName
                    endLongLat = (Lat, Long) + ""
                  }
                }
              }

              if (min <= 500 && endModeName.contains("地铁")){
                // TODO 出现次数入库
                hBaseUtils.put("residesCount", uid, "metro", endModeName, min + "")
              }
              if (min <= 500 && endModeName.contains("公交")){
                // TODO 出现次数入库
                hBaseUtils.put("residesCount", uid, "bus", endModeName, min + "")
              }

//              println(endLongLat, min, endModeName)
//              println(currentLat,currentLong)
            }
          }
        })
      })
























    // 启动Spark应用
    ssc.start()
    ssc.awaitTermination()
  }
}
