package com.study.kafka.client;

import org.apache.kafka.clients.producer.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class Kafka_Producer  {
    @Autowired
    private Kafka_Config kafka_config;

    public void producer() throws Exception {

        ProducerRecord<String, String> record = new ProducerRecord<>(Kafka_Config.topic, "1","hello, Kafka!");
        try {
            Producer producer= new KafkaProducer(kafka_config.customerConfigs());
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println(metadata.partition() + ":" + metadata.offset());
                    }
                }
            });
            producer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
