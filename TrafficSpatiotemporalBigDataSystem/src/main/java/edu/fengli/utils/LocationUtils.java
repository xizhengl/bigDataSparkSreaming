package edu.fengli.utils;

/**
 * 单例获取
 * @author Administrator
 */
public class LocationUtils {
    // 地球半径
    private double EARTH_RADIUS = 6378.137;
    private static LocationUtils instance;


    private LocationUtils(){

    }

    /**
     * 单例获取
     * @return
     */
    public static LocationUtils getInstance(){
        if (instance == null) {
            instance = new LocationUtils();
        }
        return instance;
    }

    public double rad(double d) {
        return d * Math.PI / 180.0;
    }

    /**
     * 通过经纬度获取距离(单位：米)
     * @param lat1
     * @param lng1
     * @param lat2
     * @param lng2
     * @return
     */
    public double getDistance(double lat1, double lng1, double lat2,
                                     double lng2) {
        double radLat1 = rad(lat1);
        double radLat2 = rad(lat2);
        double a = radLat1 - radLat2;
        double b = rad(lng1) - rad(lng2);
        double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2)
                + Math.cos(radLat1) * Math.cos(radLat2)
                * Math.pow(Math.sin(b / 2), 2)));
        s = s * EARTH_RADIUS;
        s = Math.round(s * 10000d) / 10000d;
        s = s * 1000;
        return s;
    }
    /**
     *
     * @param latitue 待测点的纬度
     * @param longitude 待测点的经度
     * @param areaLatitude1 纬度范围限制1
     * @param areaLatitude2 纬度范围限制2
     * @param areaLongitude1 经度限制范围1
     * @param areaLongitude2 经度范围限制2
     * @return
     */
    public boolean isInArea(double latitue,double longitude,double areaLatitude1,double areaLatitude2,double areaLongitude1,double areaLongitude2){
        // 如果在纬度的范围内
        if(isInRange(latitue, areaLatitude1, areaLatitude2)){
            // 如果都在东半球或者都在西半球
            if(areaLongitude1*areaLongitude2>0){
                if(isInRange(longitude, areaLongitude1, areaLongitude2)){
                    return true;
                }else {
                    return false;
                }
            }else {//如果一个在东半球，一个在西半球
                // 如果跨越0度经线在半圆的范围内
                if(Math.abs(areaLongitude1)+Math.abs(areaLongitude2)<180){
                    if(isInRange(longitude, areaLongitude1, areaLongitude2)){
                        return true;
                    }else {
                        return false;
                    }
                }else{// 如果跨越180度经线在半圆范围内
                    //东半球的经度范围left-180
                    double left = Math.max(areaLongitude1, areaLongitude2);
                    //西半球的经度范围right-（-180）
                    double right = Math.min(areaLongitude1, areaLongitude2);
                    if(isInRange(longitude, left, 180)||isInRange(longitude, right,-180)){
                        return true;
                    }else {
                        return false;
                    }
                }
            }
        }else{
            return false;
        }
    }

    public boolean isInRange(double point, double left,double right){
        if(point>=Math.min(left, right)&&point<=Math.max(left, right)){
            return true;
        }else {
            return false;
        }
    }
}
