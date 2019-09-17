package com.atguigu.kafka.client.consumer.lower;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.BrokerEndPoint;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

/**
 * 消费者低级API 可指定分区 offset
 */
public class SimpleExample {
    //保存findlader查找到的备份结点
    private List<String> m_replicaBrokers = new ArrayList<>();

    public SimpleExample() {
        m_replicaBrokers = new ArrayList<>();
    }

    public static void main(String args[]) {
        SimpleExample example = new SimpleExample();
// 最大读取消息数量
        long maxReads = Long.parseLong("3");
// 要订阅的 topic
        String topic = "test1";
// 要查找的分区
        int partition = Integer.parseInt("0");
// broker 节点的 ip
        List<String> seeds = new ArrayList<>();
        seeds.add("192.168.25.102");
        seeds.add("192.168.25.103");
        seeds.add("192.168.25.104");
// 端口
        int port = Integer.parseInt("9092");
        try {
            example.run(maxReads, topic, partition, seeds, port);
        } catch (Exception e) {
            System.out.println("Oops:" + e);
            e.printStackTrace();
        }
    }

    /**
     * 拉去消息
     * @param a_maxReads
     * @param a_topic
     * @param a_partition
     * @param a_seedBrokers
     * @param a_port
     * @throws Exception
     */
    public void run(long a_maxReads, String a_topic, int a_partition, List<String>
            a_seedBrokers, int a_port) throws Exception {
// 获取指定 Topic partition 的元数据
        PartitionMetadata metadata = findLeader(a_seedBrokers, a_port, a_topic,
                a_partition);
        if (metadata == null) {
            System.out.println("Can't find metadata for Topic and Partition. Exiting");
            return;
        }
        if (metadata.leader() == null) {
            System.out.println("Can't find Leader for Topic and Partition. Exiting");
            return;
        }
        //找到该分区的leader
        String leadBroker = metadata.leader().host();
        String clientName = "Client_" + a_topic + "_" + a_partition;
        //找到leader，创建消费者
        SimpleConsumer consumer = new SimpleConsumer(leadBroker, a_port, 100000, 64
                * 1024, clientName);
        //拉去分区最新消费进度
        long readOffset = getLastOffset(consumer, a_topic, a_partition,
                kafka.api.OffsetRequest.EarliestTime(), clientName);
        int numErrors = 0;
        //读取多条消息，判断当前leader是否会挂掉
        while (a_maxReads > 0) {
            if (consumer == null) {
                consumer = new SimpleConsumer(leadBroker, a_port, 100000, 64 * 1024,
                        clientName);
            }
            //创建获取数据的对象
            FetchRequest req = new
                    FetchRequestBuilder().clientId(clientName).addFetch(a_topic, a_partition,
                    readOffset, 100000).build();//100000是字节 不是多少条数据
            //处理响应
            FetchResponse fetchResponse = consumer.fetch(req);
            if (fetchResponse.hasError()) {
                numErrors++;
// Something went wrong!
                short code = fetchResponse.errorCode(a_topic, a_partition);
                System.out.println("Error fetching data from the Broker:" + leadBroker
                        + " Reason: " + code);
                //错误次数超过5
                if (numErrors > 5)
                    break;
                if (code == ErrorMapping.OffsetOutOfRangeCode()) {
// We asked for an invalid offset. For simple case ask for
// the last element to reset
                    readOffset = getLastOffset(consumer, a_topic, a_partition,
                            kafka.api.OffsetRequest.LatestTime(), clientName);
                    continue;
                }
                consumer.close();
                consumer = null;
                //当分区的主副本节点发生故障，客户将要找出新的主副本
                leadBroker = findNewLeader(leadBroker, a_topic, a_partition, a_port);
                continue;
            }
            numErrors = 0;
            //开始消费消息
            long numRead = 0;
            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(a_topic,
                    a_partition)) {
                //如果想做成高级API 得保存这个offset下次创建请求得获取到该offset 能存数据的地方就行
                long currentOffset = messageAndOffset.offset();
                if (currentOffset < readOffset) {
                    System.out.println("Found an old offset: " + currentOffset + " Expecting:" + readOffset);
                    continue;
                }
                readOffset = messageAndOffset.nextOffset();
                ByteBuffer payload = messageAndOffset.message().payload();
                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                System.out.println(String.valueOf(messageAndOffset.offset()) + ": " +
                        new String(bytes, "UTF-8"));
                numRead++;
                a_maxReads--;
            }
            if (numRead == 0) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
        if (consumer != null)
            consumer.close();
    }

    /**
     * 拉去分区最近的偏移量
     * @param consumer
     * @param topic
     * @param partition
     * @param whichTime
     * @param clientName
     * @return
     */
    public static long getLastOffset(SimpleConsumer consumer, String topic, int
            partition, long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic,
                partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new
                HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime,
                1));
        kafka.javaapi.OffsetRequest request = new
                kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(),
                clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);
        if (response.hasError()) {
            System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }

    /**
     * 当分区的主副本节点发生故障，客户将要找出新的主副本
     * @param a_oldLeader
     * @param a_topic
     * @param a_partition
     * @param a_port
     * @return
     * @throws Exception
     */
    private String findNewLeader(String a_oldLeader, String a_topic, int a_partition,
                                 int a_port) throws Exception {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            PartitionMetadata metadata = findLeader(m_replicaBrokers, a_port, a_topic,
                    a_partition);
            if (metadata == null) {
                goToSleep = true;
            } else if (metadata.leader() == null) {
                goToSleep = true;
            } else if (a_oldLeader.equalsIgnoreCase(metadata.leader().host()) && i ==
                    0) {
// first time through if the leader hasn't changed give
// ZooKeeper a second to recover
// second time, assume the broker did recover before failover,
// or it was a non-Broker issue
//
                goToSleep = true;
            } else {
                return metadata.leader().host();
            }
            if (goToSleep) {
                Thread.sleep(1000);
            }
        }
        System.out.println("Unable to find new leader after Broker failure. Exiting");
        throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }

    /**
     * 从broker-list中找到分区leader结点并备份follower结点 提高下次寻找效率
     * @param a_seedBrokers
     * @param a_port
     * @param a_topic
     * @param a_partition
     * @return
     */
    private PartitionMetadata findLeader(List<String> a_seedBrokers, int a_port,
                                         String a_topic, int a_partition) {
        PartitionMetadata returnMetaData = null;
        loop:
        for (String seed : a_seedBrokers) {
            SimpleConsumer consumer = null;
            try {
                //获取分区leader的消费者对象
                consumer = new SimpleConsumer(seed, a_port, 100000, 64 * 1024,
                        "leaderLookup");
                List<String> topics = Collections.singletonList(a_topic);
                //创建主题元数据请求
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
                //获取请求的返回数据
                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {//1个toic
                    for (PartitionMetadata part : item.partitionsMetadata()) {//toipc的分区
                        if (part.partitionId() == a_partition) {//找到0号分区的leader 元数据
                            returnMetaData = part;
                            break loop;
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("Error communicating with Broker [" + seed + "] to find Leader for [" + a_topic + ", " + a_partition + "]Reason: " + e);
            } finally {
                if (consumer != null)
                    consumer.close();
            }
        }
        //获取该分区leader的副本备份在list中，实际生产可以备份到其他地方
        if (returnMetaData != null) {
            m_replicaBrokers.clear();
            for (BrokerEndPoint replica : returnMetaData.replicas()) {
                m_replicaBrokers.add(replica.host());
            }
        }
        //返回该分区leader的元数据 再通过.leader方法
        return returnMetaData;
    }
}