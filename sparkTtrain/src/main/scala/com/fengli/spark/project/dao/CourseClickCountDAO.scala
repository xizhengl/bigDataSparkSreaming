package com.fengli.spark.project.dao

import com.fengli.spark.project.domain.CourseClickCount
import com.fengli.spark.project.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
 * 实战课程点击数 DAO
 */
object CourseClickCountDAO {
  val tableName = "imooc_course_clickcount2"
  val cf = "info"
  val qualifer = "click_count"


  /**
   * 保存数据到HBase
   * @param list
   */
  def save(list: ListBuffer[CourseClickCount]): Unit = {

    val table = HBaseUtils.getInstance().getHTable(tableName)

    for (ele <- list){
      table.incrementColumnValue(Bytes.toBytes(ele.day_course)
      ,Bytes.toBytes(cf)
      ,Bytes.toBytes(qualifer)
      ,ele.click_count)
    }
  }


  /**
   * 根据Rowkey查值
   * @param day_course
   */
  def find(day_course: String): Long ={
    val table = HBaseUtils.getInstance().getHTable(tableName)

    val get = new Get(Bytes.toBytes(day_course))

    val value = table.get(get).getValue(cf.getBytes()
      , qualifer.getBytes())

    if (value == null) {
      0L
    }else{
      Bytes.toLong(value)
    }
  }

  def main(args: Array[String]): Unit = {
   val list = new ListBuffer[CourseClickCount]
    list.append(CourseClickCount("2018111_8", 8)
      , CourseClickCount("2018111_8", 217921)
      , CourseClickCount("2018111_9", 78978)
      , CourseClickCount("2018111_10", 87)
      , CourseClickCount("2018111_11", 111)
    )

//    save(list)

    println(find(("2018111_8"))
      , find(("2018111_8"))
      , find(("2018111_9"))
      , find(("2018111_10"))
      , find(("2018111_11"))
    )
  }
}
