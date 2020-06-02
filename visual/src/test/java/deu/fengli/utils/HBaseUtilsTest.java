package deu.fengli.utils;

import org.junit.jupiter.api.Test;

public class HBaseUtilsTest {

    @Test
    public void test(){
        boolean exist = HBaseUtils.getInstance().isTableExist("userInfo");
        System.out.println(exist);
    }
}
