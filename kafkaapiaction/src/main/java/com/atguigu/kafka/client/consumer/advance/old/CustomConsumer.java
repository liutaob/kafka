package com.atguigu.kafka.client.consumer.advance.old;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * 过时的消费者API 高級
 */
public class CustomConsumer {
    @SuppressWarnings("deprecation")
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", "192.168.25.130:2182");
        properties.put("group.id", "g1");
        properties.put("zookeeper.session.timeout.ms", "500");
        properties.put("zookeeper.sync.time.ms", "250");
        properties.put("auto.commit.interval.ms", "1000");
// 创建消费者连接器
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new
                ConsumerConfig(properties));
        HashMap<String, Integer> topicCount = new HashMap<>();
        topicCount.put("first2", 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
                consumer.createMessageStreams(topicCount);
        KafkaStream<byte[], byte[]> stream = consumerMap.get("first2").get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            System.out.println(new String(it.next().message()));
        }
    }
}