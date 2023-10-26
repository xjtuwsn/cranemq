package com.github.xjtuwsn.cranemq.example.service;

import com.github.xjtuwsn.cranemq.client.spring.annotation.CraneMQListener;
import com.github.xjtuwsn.cranemq.example.entity.User;
import org.springframework.stereotype.Service;

/**
 * @project:cranemq
 * @file:MessageConsumer
 * @author:wsn
 * @create:2023/10/25-22:23
 */
@Service
public class MessageConsumer {

    @CraneMQListener(id = "0", topic = "springTopic", tag = "*", ordered = false, dataType = User.class)
    public void receiver(User message) {
        System.out.println(message);
    }

    @CraneMQListener(id = "1", topic = "springTopic", tag = "*", ordered = true, dataType = User.class)
    public void orderedReceiver(User message) {
        System.out.println(message);
    }
}
