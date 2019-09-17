package com.atguigu.kafka.client.provider.old;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * 过时的API
 */
public class OldProducer {
    @SuppressWarnings("deprecation")
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("metadata.broker.list", "192.168.25.103:9092");
        properties.put("request.required.acks", "1");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
//        properties.put("partitioner.class", "com.atguigu.kafka.client.provider.old.CustomPartitioner");
        Producer<Integer, String> producer = new Producer<Integer, String>(new
                ProducerConfig(properties));
        KeyedMessage<Integer, String> message = new KeyedMessage<Integer,
                String>("test1", "121456");
        producer.send(message);
        System.out.println("消息发送成功");
    }
}