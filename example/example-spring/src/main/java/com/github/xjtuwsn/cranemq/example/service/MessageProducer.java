package com.github.xjtuwsn.cranemq.example.service;

import com.github.xjtuwsn.cranemq.client.producer.MQSelector;
import com.github.xjtuwsn.cranemq.client.producer.result.SendResult;
import com.github.xjtuwsn.cranemq.client.spring.template.CraneMQTemplate;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.github.xjtuwsn.cranemq.example.entity.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.TimeUnit;

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
        SendResult send = craneMQTemplate.send("springTopic", "", new User("test", 12));
    }

    public void sendMessageOrder() {
        for (int i = 0; i < 10; i++) {
            SendResult send = craneMQTemplate.send("springTopic", "", new User("test", i), new MQSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> queues, Object arg) {
                    return queues.get(0);
                }
            }, i);

        }
    }

    public void sendMessageDelay(int delay, TimeUnit unit) {
        craneMQTemplate.send("springTopic", "", new User("delay", 111), delay, TimeUnit.SECONDS);
    }
}
