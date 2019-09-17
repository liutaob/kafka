package com.study.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class StudySpringbootKafkaApplication {

    public static void main(String[] args) {
        SpringApplication.run(StudySpringbootKafkaApplication.class, args);
    }

}
