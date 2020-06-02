package deu.fengli.daoimpl;

import deu.fengli.dao.UserInfoDAO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 用户信息表数据处理接口实现类
 * @author Administrator
 */
@Component
public class UserInfoDaoImpl implements UserInfoDAO {

    private Connection connection;
    private Configuration configuration;

    @Autowired
    public UserInfoDaoImpl(Connection connection, Configuration configuration) {
        this.connection = connection;
        this.configuration = configuration;
    }

    @Override
    public Long countUser(String tableName) {
        long count = 0;
        try {
            TableName name=TableName.valueOf(tableName);
            Scan scan = new Scan();
            AggregationClient aggregationClient = new AggregationClient(configuration);
            count = aggregationClient.rowCount(name, new LongColumnInterpreter(), scan);
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return count;
    }

    @Override
    public List<String> findAllRowKeyAndColumnValue(String tableName, String cf, String column) {
        List<String> list = new ArrayList<>();
        TableName t = TableName.valueOf(tableName);
        Scan scan = new Scan();
        try {
            Table table = connection.getTable(t);
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
}
