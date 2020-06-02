package com.fengli.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * HBase 工具类
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

    /**
     * 根据表名和输入条件获取HBase的记录数
     * @param tableName
     * @param condition
     * @return
     */
    public Map<String, Long> query(String tableName, String condition) throws IOException {
        Map<String, Long> map = new HashMap<String, Long>();
        HTable hTable = getHTable(tableName);
        String cf = "info";
        String qualifier = "click_count";
        Scan scan = new Scan();

        Filter filter = new PrefixFilter(Bytes.toBytes(condition));
        scan.setFilter(filter);

        ResultScanner rs = hTable.getScanner(scan);

        for (Result result: rs){
            String rowKey = Bytes.toString(result.getRow());
            long clickCount = Bytes.toLong(result.getValue(cf.getBytes(), qualifier.getBytes()));
            map.put(rowKey,clickCount);
        }

        return map;
    }

    public static void main(String[] args) throws IOException {
        Map<String, Long> map = HBaseUtils.getInstance().query("imooc_course_clickcount2", "20200210");

        for (Map.Entry<String, Long> entry : map.entrySet()){
            System.out.println(entry.getKey());
            System.out.println(entry.getValue());
        }
    }
}
