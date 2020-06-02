package com.fengli.spark.project.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Date;

/**
 * HBase操作工具类 : 单例模式进行封装
 * @author Administrator
 */
public class HBaseUtils {
    HBaseAdmin admin = null;
    Configuration configuration = null;

    private HBaseUtils(){
        configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum","192.168.10.133:2181");
        configuration.set("hbase.rootdir","hdfs://hadoop001:8020/hbase");


        try {
            admin = new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static HBaseUtils instance = null;

    public static synchronized HBaseUtils getInstance(){
        if (instance == null){
            instance = new HBaseUtils();
        }
        return instance;
    }


    /**
     * 根据表名获取HTable实例
     * @param tableName
     * @return
     */
    public HTable getHTable(String tableName){
        HTable table = null;

        try {
            table = new HTable(configuration, tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    /**
     * 添加一条记录到HBase
     * @param tableName 表名
     * @param rowKey HBase表的rowky
     * @param cf HBase表的columnfamily
     * @param column HBase表的列
     * @param value 写入HBase表的值
     */
    public void put(String tableName, String rowKey, String cf , String column, String value){
        HTable table = getHTable(tableName);

        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));

        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        HBaseUtils hBaseUtils = HBaseUtils.getInstance();
        HTable table = hBaseUtils.getHTable("imooc_course_clickcount");
        System.out.println(table.getName().getNameAsString());

//        HBaseUtils.getInstance().put("imooc_course_clickcount2"
//                ,"20181111_2"
//                , "info"
//                , "click_count"
//                , "12");
    }
}
