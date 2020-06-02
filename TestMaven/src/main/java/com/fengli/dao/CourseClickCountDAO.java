package com.fengli.dao;

import com.fengli.domain.CourseClickCount;
import com.fengli.utils.HBaseUtils;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 实战课程访问数据访问层
 * @author Administrator
 */
@Component
public class CourseClickCountDAO {

    /**
     * 根据天查询
     * @param day
     * @return
     */
    public List<CourseClickCount> query(String day) throws IOException {
        List<CourseClickCount> list = new ArrayList<CourseClickCount>();

        // HBase表中根据天获取对应的访问量
        Map<String, Long> map = HBaseUtils.getInstance().query("imooc_course_clickcount2", day);

        for (Map.Entry<String, Long> entry:map.entrySet()){
            CourseClickCount model = new CourseClickCount();
            model.setName(entry.getKey());
            model.setValue(entry.getValue());
            list.add(model);
        }
        return list;
    }

    public static void main(String[] args) throws IOException {
        CourseClickCountDAO courseClickCountDAO = new CourseClickCountDAO();
        List<CourseClickCount> list = courseClickCountDAO.query("20200210");

        for (CourseClickCount c :
                list) {
            System.out.println(c.getName() + ":" + c.getValue());
        }
    }
}
