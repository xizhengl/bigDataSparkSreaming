package java.com.fengli.log;

import org.apache.log4j.Logger;

/**
 * 模拟日志产生
 * @author Administrator
 */
public class LoggerGenerator {

    private static Logger logger = Logger.getLogger(LoggerGenerator.class.getName());

    public static void main(String[] args) throws InterruptedException {

        int index = 0;
        while (true){
            Thread.sleep(1000);
            logger.info("current value is :" + index++);
        }
    }
}
