package com.github.xjtuwsn.cranemq.test.performance;

import com.github.xjtuwsn.cranemq.client.consumer.DefaultPushConsumer;
import com.github.xjtuwsn.cranemq.client.consumer.listener.CommonMessageListener;
import com.github.xjtuwsn.cranemq.common.consumer.MessageModel;
import com.github.xjtuwsn.cranemq.common.consumer.StartConsume;
import com.github.xjtuwsn.cranemq.common.entity.ReadyMessage;
import com.github.xjtuwsn.cranemq.common.remote.RemoteHook;

import java.util.List;

/**
 * @project:cranemq
 * @file:PushConsumerTest
 * @author:wsn
 * @create:2023/10/08-19:09
 */
public class PushConsumerTest {
    public static void main(String[] args) {
        DefaultPushConsumer.builder()
                .consumerId("1")
                .consumerGroup("group_push")
                .bindRegistry("127.0.0.1:11111")
                .messageModel(MessageModel.BRODERCAST)
                .startConsume(StartConsume.FROM_FIRST_OFFSET)
                .subscribe("topic1", "*")
                .messageListener(new CommonMessageListener() {
                    @Override
                    public boolean consume(List<ReadyMessage> messages) {
                        for (ReadyMessage message : messages) {
                            int queueId = message.getQueueId();
                            String content = new String(message.getBody());
                            System.out.println("queueId: " + queueId + ", content: " + content);
                        }
                        return true;
                    }
                }).build().start();

//        DefaultPushConsumer defaultPushConsumer = new DefaultPushConsumer("group_push");
//        defaultPushConsumer.bindRegistry("127.0.0.1:11111");
//        defaultPushConsumer.subscribe("topic2", "*");
//        defaultPushConsumer.registerListener(new CommonMessageListener() {
//            @Override
//            public boolean consume(List<ReadyMessage> messages) {
//                for (ReadyMessage message : messages) {
//                    int queueId = message.getQueueId();
//                    String content = new String(message.getBody());
//                    System.out.println("queueId: " + queueId + ", content: " + content);
//                }
//                return true;
//            }
//        });
//        defaultPushConsumer.start();
    }
}
