package com.fengli.spark

import java.sql.{Connection, DriverManager}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 使用Spark Streaming 完成词频统计
 */
object StatefulWordCount {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("StatefulWordCount")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 如果使用了带状态的算子，必须设置checkpoint 生成环境中设置在HDFS一个文件中
    ssc.checkpoint(".")

    val lines = ssc.socketTextStream("192.168.10.134", 6789)

    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)


    //    state.print()
    //TODO... 将结果写入MySQL
//    result.foreachRDD{
//      rdd =>
//        val connection = createConnection()  // executed at the driver
//        rdd.foreach { record =>
//          val sql = "insert into wordcount(word, wordcount) values(' "+ record._1 + "'," + record._2 + ")"
//          connection.createStatement().execute(sql)
//        }
//    }

    result.foreachRDD{ rdd =>
        rdd.foreachPartition { partitionOfRecords =>
            val connection = createConnection()
            partitionOfRecords.foreach(pair => {
              val sql = "insert into wordcount(word, wordcount) values(' "+ pair._1 + "'," + pair._2 + ")"
              connection.createStatement().execute(sql)
            })
            connection.close()
        }
    }

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 获取mysql连接
   * @return
   */
  def createConnection(): Connection = {
    Class.forName("com.mysql.cj.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/test?useSSL=false&serverTimezone=UTC", "root", "root")
  }
}
