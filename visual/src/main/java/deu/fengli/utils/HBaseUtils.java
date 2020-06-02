package deu.fengli.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * HBase操作工具类 : 单例模式进行封装
 * @author Lixizheng
 */
public class HBaseUtils {
    private Connection connection;
    private Admin admin = null;
    private Configuration configuration = null;

    private HBaseUtils() {
        configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum", "192.168.10.133:2181");
        configuration.set("hbase.rootdir", "hdfs://hadoop001:8020/hbase");

        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static HBaseUtils instance = null;

    /**
     * 单例模式获取实例对象
     * @return
     */
    public static synchronized HBaseUtils getInstance() {
        if (instance == null) {
            instance = new HBaseUtils();
        }
        return instance;
    }

    /**
     * 关闭连接
     */
    public void close(){
        if (admin != null){
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (connection != null){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 根据表名获取 HTable实例
     * @param tableName
     * @return
     */
    public Table getTable(String tableName) {
        TableName t = TableName.valueOf(tableName);
        Table table = null;
        try {
            table = connection.getTable(t);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    /**
     * 添加一条记录到HBase
     *
     * @param tableName 表名
     * @param rowKey    HBase表的rowKey
     * @param cf        HBase表的columnFamily
     * @param column    HBase表的列
     * @param value     写入HBase表的值
     */
    public void put(String tableName, String rowKey, String cf, String column, String value) {
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));
        try {
            Table table = getTable(tableName);
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除 HBase表
     * @param tableNames List 表名
     */
    public void deleteTables(List<String> tableNames){

        for (String tableName : tableNames) {
            try {
                TableName t = TableName.valueOf(tableName);
                admin.disableTable(t);
                admin.deleteTable(t);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建表指定表名和单个列簇名
     * @param tableName
     * @param cf
     * @throws IOException
     */
    public void createTable(String tableName, String cf) throws IOException {
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor addFamily = new HColumnDescriptor(cf.getBytes());
        desc.addFamily(addFamily);

        TableName t = TableName.valueOf(tableName);
        if (admin.tableExists(t)) {
            admin.disableTable(t);
            admin.deleteTable(t);
        }
        admin.createTable(desc);
    }

    /**
     * 重载方法
     * 创建表指定表名和多个列簇名
     * @param cfs 列族字符集合
     * @throws IOException
     */
    public void createTable(String tableName, List<String> cfs) throws IOException {
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
        for (String s : cfs) {
            HColumnDescriptor addFamily = new HColumnDescriptor(s.getBytes());
            desc.addFamily(addFamily);
        }

        TableName t = TableName.valueOf(tableName);
        if (admin.tableExists(t)) {
            admin.disableTable(t);
            admin.deleteTable(t);
        }
        admin.createTable(desc);
    }

    /**
     * 根据表名，rowKey，列簇名，列名获取值
     *
     * @param tableName 表名
     * @param rowKey    相当于关系型数据库中的主键
     * @param cf        列簇名
     * @param column    列名
     * @return 列的值
     * @throws Exception
     */
    public String findByRowKeyGetColumnValue(String tableName, String rowKey,
                                             String cf, String column) throws Exception {
        Get get = new Get(rowKey.getBytes());

        // 指定要查询的列，选加，不加类似于select *
        get.addColumn(cf.getBytes(), column.getBytes());
        TableName t = TableName.valueOf(tableName);
        Cell cell = null;
        String value = "";
        if (!connection.isClosed()) {
            Table table = connection.getTable(t);
            Result result = table.get(get);
            cell = result.getColumnLatestCell(cf.getBytes(), column.getBytes());
        }
        if (cell != null){
            value = new String(CellUtil.cloneValue(cell));
        }else {
            value = null;
        }
        return value;
    }

    /**
     * 判断表是否存在
     * @param tableName
     * @return
     */
    public boolean isTableExist(String tableName){
        boolean b = false;
        TableName t = TableName.valueOf(tableName);
        try {
            b = admin.tableExists(t);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return b;
    }

    /**
     * 获取HBase中指定表的总行数
     * @param tableName
     * @return
     */
    public long rowCountByCoprocessor(String tableName){
        long count = 0;
        try {
            TableName name=TableName.valueOf(tableName);
            // 先disable表，添加协处理器后再enable表
            admin.disableTable(name);
            HTableDescriptor descriptor = admin.getTableDescriptor(name);
            String coprocessorClass = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";
            if (! descriptor.hasCoprocessor(coprocessorClass)) {
                descriptor.addCoprocessor(coprocessorClass);
            }
            admin.modifyTable(name, descriptor);
            admin.enableTable(name);

            Scan scan = new Scan();
            AggregationClient aggregationClient = new AggregationClient(configuration);
            count = aggregationClient.rowCount(name, new LongColumnInterpreter(), scan);
        } catch (Throwable e) {
            e.printStackTrace();
        }

        return count;
    }

    /**
     * 根据表名，列簇，列名查询出该列所有数据
     * @param tableName 表名
     * @param column 列名
     * @param cf 列簇
     * @return
     */
    public List<String> getTableColumnAllValue(String tableName, String column, String cf){
        Table hTable = getTable(tableName);
        List<String> list = new ArrayList<String>();
        Scan scan = new Scan();
        try {
            ResultScanner resultScanner = hTable.getScanner(scan);
            for (Result r: resultScanner){
                byte[] row = r.getRow();
                Get get = new Get(row);

                // 指定要查询的列，选加，不加类似于select *
                get.addColumn(cf.getBytes(), column.getBytes());
                Result result = getTable(tableName).get(get);

                Cell cell = result.getColumnLatestCell(cf.getBytes(), column.getBytes());

                String value = new String(CellUtil.cloneValue(cell));

                list.add(value);
            }
        }catch (IOException e) {
        e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    /**
     * 指定表名、列簇、列名获取表中全部RowKey和值
     * @param tableName
     * @param cf
     * @param column
     * @return
     */
    public List<String> getTableAllRowKeyAndValue(String tableName, String cf, String column){
        List<String> list = new ArrayList<String>();
        Table table = getTable(tableName);
        Scan scan = new Scan();
        try {
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result r : resultScanner) {
                String rowKey = new String(r.getRow());
                Get get = new Get(r.getRow());
                get.addColumn(cf.getBytes(), column.getBytes());

                Result result = table.get(get);
                Cell cell = result.getColumnLatestCell(cf.getBytes(), column.getBytes());
                String value = new String(CellUtil.cloneValue(cell));

                list.add(rowKey + "-" + value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

    /**
     * 清空指定表中数据
     * O(n)
     * @param list
     */
    public void truncateTables(List<String> list){
        for (String tableName : list){
            Table table = getTable(tableName);
            Scan scan = new Scan();
            ResultScanner resultScanner = null;
            try {
                resultScanner = table.getScanner(scan);
                for (Result result : resultScanner){
                    Delete delete = new Delete(result.getRow());
                    table.delete(delete);
                    table.close();
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        System.out.println(HBaseUtils.getInstance().rowCountByCoprocessor("userInfo"));
    }
}
