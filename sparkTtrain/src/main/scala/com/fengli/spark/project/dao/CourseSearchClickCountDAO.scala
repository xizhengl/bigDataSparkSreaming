package com.fengli.spark.project.dao

import com.fengli.spark.project.domain.{CourseClickCount, CourseSearchClickCount}
import com.fengli.spark.project.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
 * 实战课程点击数 DAO
 */
object CourseSearchClickCountDAO {
  val tableName = "imooc_course_search_clickcount"
  val cf = "info"
  val qualifer = "click_count"


  /**
   * 保存数据到HBase
   * @param list CourseSearchClickCount
   */
  def save(list: ListBuffer[CourseSearchClickCount]): Unit = {

    val table = HBaseUtils.getInstance().getHTable(tableName)

    for (ele <- list){
      table.incrementColumnValue(Bytes.toBytes(ele.day_search_course)
      ,Bytes.toBytes(cf)
      ,Bytes.toBytes(qualifer)
      ,ele.click_count)
    }
  }


  /**
   * 根据Rowkey查值
   * @param day_search
   */
  def find(day_search: String): Long ={
    val table = HBaseUtils.getInstance().getHTable(tableName)

    val get = new Get(Bytes.toBytes(day_search))

    val value = table.get(get).getValue(cf.getBytes()
      , qualifer.getBytes())

    if (value == null) {
      0L
    }else{
      Bytes.toLong(value)
    }
  }

  def main(args: Array[String]): Unit = {
   val list = new ListBuffer[CourseSearchClickCount]
    list.append(CourseSearchClickCount("2018111_www.baidu.com_8", 8)
      , CourseSearchClickCount("2018111_cn.bing.com_9", 217921)
    )

    save(list)

    println(find("2018111_www.baidu.com_8")
      , find("2018111_cn.bing.com_9")
    )
  }
}
