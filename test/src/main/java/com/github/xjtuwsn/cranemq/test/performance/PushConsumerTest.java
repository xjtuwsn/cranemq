package com.github.xjtuwsn.cranemq.test.performance;

import com.github.xjtuwsn.cranemq.client.consumer.DefaultPushConsumer;
import com.github.xjtuwsn.cranemq.client.consumer.listener.CommonMessageListener;
import com.github.xjtuwsn.cranemq.common.entity.ReadyMessage;

import java.util.List;

/**
 * @project:cranemq
 * @file:PushConsumerTest
 * @author:wsn
 * @create:2023/10/08-19:09
 */
public class PushConsumerTest {
    public static void main(String[] args) {
        DefaultPushConsumer defaultPushConsumer = new DefaultPushConsumer("group_push");
        defaultPushConsumer.bindRegistry("127.0.0.1:11111");
        defaultPushConsumer.subscribe("topic1", "*");
        defaultPushConsumer.registerListener(new CommonMessageListener() {
            @Override
            public boolean consume(List<ReadyMessage> messages) {
                return false;
            }
        });
        defaultPushConsumer.start();
    }
}
