package com.atguigu.kafka.client.provider.newer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 自定义指定分区的生产者
 */
public class PartitionerProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
// Kafka 服务端的主机名和端口号
        props.put("bootstrap.servers", "192.168.25.103:9092");
// 等待所有副本节点的应答
        props.put("acks", "all");
// 消息发送最大尝试次数
        props.put("retries", 0);
// 一批消息处理大小
        props.put("batch.size", 16384);
// 增加服务端请求延时
        props.put("linger.ms", 1);
// 发送缓存区内存大小
        props.put("buffer.memory", 33554432);
// key 序列化
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
// value 序列化
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
// 自定义分区
        props.put("partitioner.class", "com.atguigu.kafka.client.provider.newer.CustomPartitioner");
        Producer<String, String> producer = new KafkaProducer<>(props);
        producer.send(
                new ProducerRecord<String, String>("first2", "1", "atguigu"));
        producer.close();
    }
}