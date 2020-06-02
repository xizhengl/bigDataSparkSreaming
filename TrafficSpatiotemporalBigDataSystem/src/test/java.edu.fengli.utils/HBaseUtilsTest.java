import edu.fengli.utils.HBaseUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class HBaseUtilsTest {


    /**
     * @Date 2020/2/16
     * @result 测试成功
     * @info 测试 HBaseUtils工具类中createTable()功能：在HBase中创建一张数据表
     */
    @Test
    public void createHBaseTableTest(){
        String table = "test1";
        String cf = "info";
        try {
            HBaseUtils.getInstance().createTable(table,cf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @Date 2020/2/16
     * @result 测试成功
     * @info 测试 HBaseUtils工具类中put()功能：HBase中没有创建表无法插入数据
     */
    @Test
    public void notTablePutDataTest(){
        String tableName = "test2";
        String rowKey = "001";
        String cf = "info";
        String column = "username";
        String value = "lxz";
        HBaseUtils.getInstance().put(tableName,rowKey,cf,column,value);
    }

    /**
     * @Date 2020/2/16
     * @result 测试成功
     * @info 测试 HBaseUtils工具类中put()功能：向HBase中添加数据
     */
    @Test
    public void putDataTest(){
        String tableName = "test";
        String rowKey = "001";
        String cf = "info1";
        String column = "username";
        String value = "lxz";
//        HBaseUtils.getInstance().put(tableName,rowKey,cf,column,value);
    }

    /**
     * @Date 2020/2/16
     * @result 测试成功
     * @info 测试 HBaseUtils工具类中findByRowKeyGetColumnValue()功能：根据表名、rowKey、列簇、列名获取值
     */
    @Test
    public void byTableNameAndColumnGetValueTest(){
        String tableName = "test";
        String rowKey = "001";
        String cf = "info1";
        String column = "username";
        try {
            String val = HBaseUtils.getInstance()
                    .findByRowKeyGetColumnValue(tableName, rowKey, cf, column);
            System.out.println(val);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * @Date 2020/2/17
     * @result 测试成功
     * @info 测试 HBaseUtils工具类中isTableExist()功能 ：HBase表是否存在
     */
    @Test
    public void isTableExist(){
        boolean exist = HBaseUtils.getInstance().isTableExist("tripStaticData");
        System.out.println(exist);
    }

    /**
     * @Date 2020/2/17
     * @result 测试成功
     * @info 测试 HBaseUtils工具类中rowCountByCoprocessor()功能 ：HBase表总行数
     */
    @Test
    public void countRow(){
        long count = 0L;
        try {
             count = HBaseUtils.getInstance().rowCountByCoprocessor("test");
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.out.println(count);
    }

    /**
     * @Date 2020/2/17
     * @result 测试成功
     * @info 测试 HBaseUtils工具类中getTableColumnAllValue()
     * 功能 ：根据表名，列簇，列名查询出该列所有数据
     */
    @Test
    public void getTableColumnAllValueTest(){
        String tableName = "baseLongLatData";
        String cf = "info";
        String column = "longLat";
        List<String> list = HBaseUtils.getInstance()
                .getTableColumnAllValue(tableName, column, cf);

        for (String value : list) {
            System.out.println(value);
        }
        System.out.println(list.size());
    }

    /**
     * @Date 2020/2/17
     * @result 测试成功
     * @info 测试 HBaseUtils工具类中createTable()
     * 功能 ：创建一张HBase表， 指定多个列簇
     */
    @Test
    public void createTableTest() throws IOException {
        List<String> list = new ArrayList<String>() ;
//        list.add("info1");
//        list.add("info2");
//        HBaseUtils.getInstance().createTable("test",list);
    }

    /**
     * @Date 2020/2/18
     * @result 测试成功
     * @info 测试 HBaseUtils工具类中deleteTablesTest()
     * 功能 ：删除指定集合中所有HBase表 list<String>
     */
    @Test
    public void deleteTablesTest(){
        List<String> list = new ArrayList<String>();
//        list.add("member");
//        list.add("imooc_course_clickcount");
//        list.add("imooc_course_clickcount2");
//        list.add("imooc_course_search_clickcount");
//        list.add("test");
//        list.add("test3");
        HBaseUtils.getInstance().deleteTables(list);
    }

    /**
     * @Date 2020/2/18
     * @result 测试成功
     * @info 测试 HBaseUtils工具类中getTableAllRowKeyAndValueTest()
     * 功能 ：查询指定表中全部RowKey以及指定列的数据
     */
    @Test
    public void getTableAllRowKeyAndValueTest(){
        String tableName = "residesCount";
        String cf = "info";
        String column = "bus";

        List<String> list = HBaseUtils.getInstance().getTableAllRowKeyAndValue(tableName, cf, column);
        for (String s: list) {
            System.out.println(s);
        }
    }
    @Test
    public void truncateTablesTest(){
        List<String> list = new ArrayList<String>();
//        list.add("userInfo");
//        list.add("residesCount");
        HBaseUtils.getInstance().truncateTables(list);
    }
}
