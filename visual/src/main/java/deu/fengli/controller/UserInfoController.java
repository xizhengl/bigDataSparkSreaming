package deu.fengli.controller;

import deu.fengli.service.UserInfoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;


/**
 * @author Administrator
 */
@RestController
public class UserInfoController {

    private UserInfoService userInfoService;

    @Autowired
    public void setUserInfoService(UserInfoService userInfoService){
        this.userInfoService = userInfoService;
    }

    @GetMapping("/countUser")
    public Long countUser(){
        return  userInfoService.findUserCount();
    }

}
