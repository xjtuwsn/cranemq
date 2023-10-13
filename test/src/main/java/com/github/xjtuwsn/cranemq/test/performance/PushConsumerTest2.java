package com.github.xjtuwsn.cranemq.test.performance;

import com.github.xjtuwsn.cranemq.client.consumer.DefaultPushConsumer;
import com.github.xjtuwsn.cranemq.client.consumer.listener.CommonMessageListener;
import com.github.xjtuwsn.cranemq.client.consumer.listener.OrderedMessageListener;
import com.github.xjtuwsn.cranemq.common.consumer.MessageModel;
import com.github.xjtuwsn.cranemq.common.consumer.StartConsume;
import com.github.xjtuwsn.cranemq.common.entity.ReadyMessage;

import java.util.List;

/**
 * @project:cranemq
 * @file:PushConsumerTest2
 * @author:wsn
 * @create:2023/10/11-15:09
 */
public class PushConsumerTest2 {
    public static void main(String[] args) {
        DefaultPushConsumer.builder()
                .consumerId("2")
                .consumerGroup("group_push")
                .bindRegistry("127.0.0.1:11111")
                .messageModel(MessageModel.CLUSTER)
                .startConsume(StartConsume.FROM_LAST_OFFSET)
                .subscribe("topic2", "*")
                .messageListener(new OrderedMessageListener() {
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
    }
}
