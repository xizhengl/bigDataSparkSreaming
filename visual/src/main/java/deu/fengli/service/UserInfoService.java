package deu.fengli.service;


/**
 * 用户信息表业务处理接口
 * @author Administrator
 */
public interface UserInfoService {

    /**
     * 查询用户信息表中用户总数
     * @return 统计总数
     */
    Long findUserCount();
}
