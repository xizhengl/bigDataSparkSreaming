package com.fengli.spark.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * kafka 消费者
 * @author Administrator
 */
public class KafkaConsumer extends Thread{
    private String topic;

    public KafkaConsumer(String topic){
        this.topic = topic;
    }

    private ConsumerConnector crateConnector(){
        Properties properties = new Properties();
        properties.put("zookeeper.connect",KafkaProperties.ZK);
        properties.put("group.id",KafkaProperties.GROUP_ID);
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
    }

    @Override
    public void run() {
        ConsumerConnector consumerConnector = crateConnector();
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);
//        topicCountMap.put(topic1, 1);
//        topicCountMap.put(topic2, 1);

        // String：topic  List<KafkaStream<byte[], byte[]>> :数据流
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumerConnector.createMessageStreams(topicCountMap);

        // 获取我们每次接受到的数据
        KafkaStream<byte[], byte[]> messageAndMetadata = messageStreams.get(topic).get(0);

        ConsumerIterator<byte[], byte[]> iterator = messageAndMetadata.iterator();


        while (iterator.hasNext()){
            String message  = new String(iterator.next().message());
            System.out.println("rec:"+ message);
        }
    }
}
