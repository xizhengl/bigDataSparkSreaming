package com.fengli.spark.web;

import com.fengli.dao.CourseClickCountDAO;
import com.fengli.domain.CourseClickCount;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * web 层
 * @author Administrator
 */
@RestController
public class ImoocStatApp {
    private static Map<String, String> course = new HashMap<String,String>();
    static {
        course.put("130", "Spark SQL项目实战");
        course.put("131", "Hadoop 入门");
        course.put("128", "Spark Streaming 项目实战");
        course.put("112", "大数据面试题");
        course.put("145", "Strom项目实战");
        course.put("146", "Strom项目实战2");
    }

    @Autowired
    CourseClickCountDAO countDAO;

//    @RequestMapping(value = "/courseClick_dynamic", method = RequestMethod.GET)
//    public ModelAndView courseClickCount() throws Exception{
//        ModelAndView view = new ModelAndView();
//
//        List<CourseClickCount> list = countDAO.query("20200210");
//        for (CourseClickCount model :
//                list) {
//            model.setName(course.get(model.getName().substring(9)));
//        }
//        JSONArray json = JSONArray.fromObject(list);
//
//        view.addObject("data_json", json);
//
//        return view;
//    }


    @ResponseBody
    @RequestMapping(value = "/courseClick_dynamic", method = RequestMethod.POST)
    public List<CourseClickCount> courseClickCount() throws Exception{
        List<CourseClickCount> list = countDAO.query("20200210");
        for (CourseClickCount model : list) {
            model.setName(course.get(model.getName().substring(9)));
        }
        return list;
    }


    @RequestMapping(value = "/echars", method = RequestMethod.GET)
    public ModelAndView echars(){
        return new ModelAndView("echars");
    }
}
