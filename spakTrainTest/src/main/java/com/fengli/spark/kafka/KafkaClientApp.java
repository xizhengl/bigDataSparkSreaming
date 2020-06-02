package java.com.fengli.spark.kafka;

import java.com.fengli.spark.kafka.KafkaProducer;
/**
 * kafkaApi  Java API测试
 * @author Administrator
 */
public class KafkaClientApp {
    public static void main(String[] args) {
        new KafkaProducer(KafkaProperties.TOPIC).start();
//        new KafkaConsumer(KafkaProperties.TOPIC).start();
    }
}
