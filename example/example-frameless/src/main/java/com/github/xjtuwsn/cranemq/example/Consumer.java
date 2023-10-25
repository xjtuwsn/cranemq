package com.github.xjtuwsn.cranemq.example;

import com.github.xjtuwsn.cranemq.client.consumer.DefaultPushConsumer;
import com.github.xjtuwsn.cranemq.client.consumer.listener.CommonMessageListener;
import com.github.xjtuwsn.cranemq.client.consumer.rebalance.GrayQueueAllocation;
import com.github.xjtuwsn.cranemq.common.consumer.MessageModel;
import com.github.xjtuwsn.cranemq.common.consumer.StartConsume;
import com.github.xjtuwsn.cranemq.common.entity.ReadyMessage;
import com.github.xjtuwsn.cranemq.common.remote.enums.RegistryType;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @project:cranemq
 * @file:Consumer
 * @author:wsn
 * @create:2023/10/25-22:04
 */
public class Consumer {

    public static void main(String[] args) {
        DefaultPushConsumer.builder()
                .consumerId("1")
                .consumerGroup("group_test")
                .bindRegistry("127.0.0.1:8848")
                .messageModel(MessageModel.CLUSTER) // .gray(true).queueAllocation(new GrayQueueAllocation(1))   -->gray
                .registryType(RegistryType.NACOS)
                .startConsume(StartConsume.FROM_FIRST_OFFSET)
                .subscribe("testTopic", "*")
                .messageListener(new CommonMessageListener() {
                    @Override
                    public boolean consume(List<ReadyMessage> messages) {
                        for (ReadyMessage message : messages) {
                            int queueId = message.getQueueId();
                            String content = new String(message.getBody());
                            System.out.println("queueId: " + queueId + ", content: " + content +
                                    ", current is " + TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) +
                                    ", retry: " + message.getRetry());
                        }
                        return true;
                    }
                }).build().start();
    }
}
