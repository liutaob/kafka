package com.study.kafka.template;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Date;


@Component
public class KafkaProducer {

    private static final String MY_TOPIC = "test1";

    @Autowired
    KafkaTemplate kafkaTemplate;

    public void produce(){
        Message message = new Message();
        message.setId(12L);
        message.setMsg("hello jack");
        message.setTime(new Date());
//        kafkaTemplate.send(MY_TOPIC,message);//结合@Listener注解用 测试方法报转换异常。 因为消息编解码是字符串序列化
        kafkaTemplate.send(new ProducerRecord<String, String>("test1",
                "hello world-"));
    }
}