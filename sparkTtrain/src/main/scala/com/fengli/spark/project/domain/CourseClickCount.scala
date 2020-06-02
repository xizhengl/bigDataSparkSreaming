package com.fengli.spark.project.domain

/**
 * 实战课程点击数
 * @param day_course HBase中的rowkey, 20181111_1
 * @param click_count 20181111_1 的点击总数
 */
case class CourseClickCount(day_course:String, click_count:Long)
