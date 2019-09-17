package com.study.kafka;

import com.study.kafka.client.Kafka_Consumer;
import com.study.kafka.client.Kafka_Producer;
import com.study.kafka.template.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class StudySpringbootKafkaApplicationTests {
    //springboot方式temlate
    @Autowired
    private KafkaProducer kafkaProducer;
    @Autowired
    private KafkaTemplate kafkaTemplate;
    //原生客户端
    @Autowired
    private Kafka_Producer kafka_producer;
    @Autowired
    private Kafka_Consumer kafka_consumer;

    /***
     *  原生API方式发消息 消费自动执行
     * @throws Exception
     */
    @Test
    public void client() throws Exception {
        kafka_producer.producer();
        System.out.println("发送成功");
//        kafka_consumer.consume();
    }
    @Test
    public void client2() throws Exception {
//        kafka_producer.producer();
//        System.out.println("发送成功");
        kafka_consumer.consume();
    }

    /**
     * 以template方式发消息 和自己包装过的template
     * @throws Exception
     */
    @Test
    public void template() throws Exception {
        kafkaProducer.produce();
        kafkaTemplate.send(new ProducerRecord<String, String>("test1", "1","hello world-"));
//        kafka_producer.producer();
    }

}
