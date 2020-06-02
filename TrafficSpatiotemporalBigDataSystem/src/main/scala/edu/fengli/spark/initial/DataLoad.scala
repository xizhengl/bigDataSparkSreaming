package edu.fengli.spark.initial

import java.io.File

import edu.fengli.spark.domain.{BaseLongLatBean, TripStaticBean}
import edu.fengli.utils.HBaseUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataLoad {
  def main(args: Array[String]): Unit = {

    if (args.length != 2){
      println("User Usage： <Path1:服创大赛-出行方式静态数据.csv> \n <Path2: 服创大赛-基站经纬度数据.csv>")
      System.exit(1)
    }
    val Array(path1, path2) = args

    val start = System.currentTimeMillis()
    val longLatTable = "baseLongLatData"
    val longLatTableCf = "info"
    val tripStaticTable = "tripStaticData"
    val tripStaticTableCf = "info"

    // 在HBase中创建两张表并指定列簇
    val utils: HBaseUtils = HBaseUtils.getInstance()
    utils.createTable(longLatTable, longLatTableCf)
    utils.createTable(tripStaticTable, tripStaticTableCf)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Data")
    val ss: SparkSession = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

//    val file1 = new File("data/服创大赛-出行方式静态数据.csv")
//    val file2 = new File("data/服创大赛-基站经纬度数据.csv")
//    val path1 = file1.getAbsoluteFile.toString
//    val path2 = file2.getAbsoluteFile.toString

    val data1: DataFrame = ss.read.format("com.databricks.spark.csv")
      // 在csv第一行有属性"true"，没有就是"false"
      .option("header", "true")
      .option("inferSchema", true.toString) //这是自动推断属性列的数据类型
      .load(path1)

    val beansRDD1: RDD[TripStaticBean] = data1.rdd.map(line => {
      val strings = line.toString().split(",")
      if (strings.length == 5) {
        val long = strings(0).substring(1, strings(0).length)
        val longLat = long + "-" + strings(1)
        var modeNum = ""
        if (strings(4).equals("null]")) {
          modeNum = " "
        } else {
          modeNum = strings(4).substring(0, strings(4).length)
        }
        val mode = strings(2)
        val modeName = strings(3)
        TripStaticBean(longLat, mode, modeName, modeNum)
      } else {
        TripStaticBean("", "", "", "")
      }
    }).filter(_.longLat != null)


    beansRDD1.foreach(bean => {
      val rowKey = bean.longLat
      val mode = bean.mode
      val modeName = bean.modeName
      val modeNum = bean.modeNum

      val utils = HBaseUtils.getInstance()

      /**
       * 测试偏差2条数据 应该是添加时rowKey重复导致
       * 2020/2/17
       */

      utils.put(tripStaticTable,rowKey,tripStaticTableCf,"mode",mode)
      utils.put(tripStaticTable,rowKey,tripStaticTableCf,"modeName",modeName)
      utils.put(tripStaticTable,rowKey,tripStaticTableCf,"modeNum",modeNum)
    })

    val data2: DataFrame = ss.read.format("com.databricks.spark.csv")
      // 在csv第一行有属性"true"，没有就是"false"
      .option("header", "true")
      // 这是自动推断属性列的数据类型
      .option("inferSchema", true.toString)
      .load(path2)

    val beansRDD2: RDD[BaseLongLatBean] = data2.rdd.map(line => {
      val strings = line.toString().split(",")
      if (strings.length == 3) {
        var longLat = strings(0) + "-" + strings(1)
        longLat = longLat.substring(1, longLat.length)
        val str = strings(2).substring(0, strings(2).length - 1)
        BaseLongLatBean(str, longLat)
      }else{
        BaseLongLatBean("", "")
      }
    }).filter(_.longLat != null)

    beansRDD2.foreach(x => {
      val rowKey = x.laCi
      val longLat = x.longLat
      val column = "longLat"
      val cf = "info"

      val utils = HBaseUtils.getInstance()
      /**
       * 2020/2/17 测试成功
       */
      utils.put(longLatTable, rowKey, cf, column, longLat)
    })

    val end = System.currentTimeMillis()

    println("运行：" + (end - start) / 1000 + "秒")
  }
}
