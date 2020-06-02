package com.fengli.spark.kafka;

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
