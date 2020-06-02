package com.fengli.spark.web;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

/**
 *
 * @author Administrator
 */
@RestController
public class HelloBoot {

    @RequestMapping(value = "/hello", method = RequestMethod.GET)
    public String sayHello(){
        return "hello world Spring Boot!";
    }

    @RequestMapping(value = "/first", method = RequestMethod.GET)
    public ModelAndView firstDemo(){

        return new ModelAndView("bar");
    }

    @RequestMapping(value = "/courseClick", method = RequestMethod.GET)
    public ModelAndView courseClickCountStat(){
        return new ModelAndView("demo");
    }
}
