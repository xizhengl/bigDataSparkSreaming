package com.fengli.spark.project.utils


import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

/**
 * 日期工具类
 */
object DateUtils {
  val YYYYMMDDHHMMSS = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val TARGE_FORMAT = FastDateFormat.getInstance("yyyyMMddHHmmss")

  def getTime(time: String) ={
    YYYYMMDDHHMMSS.parse(time).getTime
  }

  def parseTOMinute(time: String) ={
    TARGE_FORMAT.format(new Date(getTime(time)))
  }

  def main(args: Array[String]): Unit = {
    println(parseTOMinute("2020-02-10 20:24:01"))
  }
}
