package com.atguigu.kafka.client.provider.newer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 自定义生产者
 */
public class CustomPartitioner implements Partitioner {
    @Override
    public void configure(Map<String, ?> configs) {
    }
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    // 控制分区
        return 1;
    }
    @Override
    public void close() {
    }
}