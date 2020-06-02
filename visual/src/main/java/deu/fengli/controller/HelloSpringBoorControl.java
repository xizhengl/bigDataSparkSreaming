package deu.fengli.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;


/**
 *
 * @author Administrator
 */
@RestController
public class HelloSpringBoorControl {

    @GetMapping("/hello")
    public String hello(){
        return "SpringBoot!";
    }

}
