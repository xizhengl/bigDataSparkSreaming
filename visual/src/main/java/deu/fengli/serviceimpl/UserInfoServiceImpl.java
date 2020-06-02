package deu.fengli.serviceimpl;

import deu.fengli.dao.UserInfoDAO;
import deu.fengli.service.UserInfoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


/**
 * 用户信息表业务处理接口实现类
 * @author Administrator
 */
@Service
public class UserInfoServiceImpl implements UserInfoService {

    private UserInfoDAO userInfoDAO;

    @Autowired
    public void setUserInfoDAO(UserInfoDAO userInfoDAO){
        this.userInfoDAO = userInfoDAO;
    }

    @Override
    public Long findUserCount() {
        return userInfoDAO.countUser("userInfo");
    }
}
