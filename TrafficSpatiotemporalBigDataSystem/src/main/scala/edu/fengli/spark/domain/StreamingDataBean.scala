package edu.fengli.spark.domain

/**
 * 流动数据实体类
 * @param uid 用户唯一标识
 * @param time 时间戳
 * @param longLat 经纬度信息
 */
case class StreamingDataBean(uid: String, time: String, longLat :String)
