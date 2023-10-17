package com.github.xjtuwsn.cranemq.test.service;

import com.github.xjtuwsn.cranemq.client.producer.MQSelector;
import com.github.xjtuwsn.cranemq.client.producer.result.SendResult;
import com.github.xjtuwsn.cranemq.client.spring.annotation.CraneMQListener;
import com.github.xjtuwsn.cranemq.client.spring.template.CraneMQTemplate;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.github.xjtuwsn.cranemq.test.entity.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @project:cranemq
 * @file:UserService
 * @author:wsn
 * @create:2023/10/14-15:44
 */
@RestController
public class UserService {
    @Autowired
    public CraneMQTemplate craneMQTemplate;
    @RequestMapping("/hello")
    public String test() {
        // SendResult send = craneMQTemplate.send("topic2", "abc", "hello");
        ExecutorService executorService = Executors.newFixedThreadPool(4);
        for (int i = 0; i < 1; i++) {
            executorService.execute(() -> {
                for (int j = 0; j < 10; j++) {
                    craneMQTemplate.send("topic16", "abc", new User("test", j), new MQSelector() {
                        @Override
                        public MessageQueue select(List<MessageQueue> queues, Object arg) {
                            return queues.get(0);
                        }
                    }, j);
                }
            });
        }
        return "ok";
    }

    @CraneMQListener(id = "0", topic = "topic16", tag = "*", ordered = false, dataType = User.class)
    public void receive(User message) {
        System.out.println(message + ", ------------------");
    }
}
