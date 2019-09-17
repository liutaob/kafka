package com.study.kafka.client;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@Component
public class Kafka_Consumer{
    @Autowired
    private Kafka_Config kafka_config;

    public void consume(){
        //每个线程一个KafkaConsumer实例，且线程数设置成分区数，最大化提高消费能力
        int consumerThreadNum = 2;//线程数设置成分区数，最大化提高消费能力
        for (int i = 0; i < consumerThreadNum; i++) {
            new KafkaConsumerThread(kafka_config.customerConfigs(), Kafka_Config.topic).start();
        }
    }

    public class KafkaConsumerThread extends Thread {
        private KafkaConsumer<String, String> kafkaConsumer;

        public KafkaConsumerThread(Properties props, String topic) {
            this.kafkaConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
            this.kafkaConsumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records =
                            kafkaConsumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println("message------------ "+record.value());
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                kafkaConsumer.close();
            }
        }
    }
}