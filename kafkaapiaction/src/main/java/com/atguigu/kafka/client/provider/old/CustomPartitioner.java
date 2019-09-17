package com.atguigu.kafka.client.provider.old;

import kafka.producer.Partitioner;

/**
 * 自定义生产者
 */
public class CustomPartitioner implements Partitioner {
    public CustomPartitioner() {
        super();
    }

    @Override
    public int partition(Object key, int numPartitions) {
// 控制分区
        return 0;
    }
}