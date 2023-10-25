package com.github.xjtuwsn.cranemq.example.service;

import com.github.xjtuwsn.cranemq.client.producer.result.SendResult;
import com.github.xjtuwsn.cranemq.client.spring.template.CraneMQTemplate;
import com.github.xjtuwsn.cranemq.example.entity.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @project:cranemq
 * @file:MessageProducer
 * @author:wsn
 * @create:2023/10/25-22:23
 */
@Service
public class MessageProducer {

    @Autowired
    public CraneMQTemplate craneMQTemplate;


    public void sendMessageSimple() {
        SendResult send = craneMQTemplate.send("testTopic", "", new User("test", 12));
    }
}
