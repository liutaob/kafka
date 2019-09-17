package com.atguigu.kafka.client.consumer.advance.newer;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/**
 * 新消费者API 高级
 */
public class CustomNewConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
// 定义 kakfa 服务的地址，不需要将所有 broker 指定上
        props.put("bootstrap.servers", "192.168.25.102:9092");
// 制定 consumer group 
        props.put("group.id", "test222");
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");//默认latest消费最新的，earliest消费最早的数据 类比--from-beginning
// 是否自动确认 offset 
        props.put("enable.auto.commit", "true");
        //自动确认offset的时间间隔
        props.put("auto.commit.interval.ms", "1000");
// key 的序列化类
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
// value 的序列化类
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
// 定义 consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
// 消费者订阅的 topic, 可同时订阅多个
//        consumer.subscribe(Arrays.asList("first2", "test"));

        //注意：提供的assgin和seek方法指定offset但是无法维护offset只能每次都重置。
        consumer.assign(Collections.singletonList(new TopicPartition("test",0)));
        consumer.seek(new TopicPartition("test",0),0);

        while (true) {
// 读取数据，读取超时时间为 100ms
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n",
                        record.offset(), record.key(), record.value());
        }
    }
}