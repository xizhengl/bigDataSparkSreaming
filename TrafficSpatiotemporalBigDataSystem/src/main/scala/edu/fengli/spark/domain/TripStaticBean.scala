package edu.fengli.spark.domain

/**
 * 出行方式静态数据实体类
 * @param longLat 经纬度数据
 * @param mode 出行方式
 * @param modeName 出行方式名称
 * @param modeNum 出行线路
 */
case class TripStaticBean(longLat: String, mode: String, modeName: String, modeNum :String)
