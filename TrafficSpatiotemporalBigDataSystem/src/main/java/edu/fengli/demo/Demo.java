package edu.fengli.demo;

import edu.fengli.utils.LocationUtils;

/**
 * @author Administrator
 */
public class Demo {
    public static void main(String[] args) {
        double lat1 = 41.83649826;
        double lng1 = 123.3514404;
        double lat2 = 41.84408951;
        double lng2 = 123.410408;
        double distance = LocationUtils.getInstance().getDistance(lat1, lng1, lat2, lng2);
        System.out.println(distance);
    }
}
