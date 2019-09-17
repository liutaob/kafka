/*
 * Copyright 2019 Guangdong Etone Technology Co.,Ltd.
 * All rights reserved.
 */
package com.study.kafka.template;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * 一句简洁的说明
 *
 * @author <a href="mailto:maxid@qq.com">LiuTao</a>
 * @since $$Id$$
 */
@Service
@Slf4j
public class BookService {
    private static final String MY_TOPIC = "test1";

    @KafkaListener(topics = {MY_TOPIC})
    public void consume(String message){
        log.info("receive msg "+ message);
    }
}
