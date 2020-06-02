package deu.fengli.dao;

import java.util.List;

/**
 * 用户信息表数据处理接口
 * @author Administrator
 */
public interface UserInfoDAO {

    /**
     * 查询用户总数
     * @param tableName 表名
     * @return 统计次数
     */
    Long countUser(String tableName);

    /**
     * 根据表信息获取表中全部rowKey、列的值
     * @param tableName 表名
     * @param cf 列簇
     * @param column 列名
     * @return list<rowKey-value>
     */
    List<String> findAllRowKeyAndColumnValue(String tableName, String cf, String column);
}
