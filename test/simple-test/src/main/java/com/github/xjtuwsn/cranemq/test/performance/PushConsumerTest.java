package com.github.xjtuwsn.cranemq.test.performance;

import com.github.xjtuwsn.cranemq.client.consumer.DefaultPushConsumer;
import com.github.xjtuwsn.cranemq.client.consumer.listener.CommonMessageListener;
import com.github.xjtuwsn.cranemq.client.consumer.listener.OrderedMessageListener;
import com.github.xjtuwsn.cranemq.client.consumer.rebalance.GrayQueueAllocation;
import com.github.xjtuwsn.cranemq.common.consumer.MessageModel;
import com.github.xjtuwsn.cranemq.common.consumer.StartConsume;
import com.github.xjtuwsn.cranemq.common.entity.ReadyMessage;
import com.github.xjtuwsn.cranemq.common.remote.RemoteHook;
import com.github.xjtuwsn.cranemq.common.remote.enums.RegistryType;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @project:cranemq
 * @file:PushConsumerTest
 * @author:wsn
 * @create:2023/10/08-19:09
 */
public class PushConsumerTest {
    public static void main(String[] args) {
        AtomicInteger sum = new AtomicInteger(0);
        DefaultPushConsumer.builder()
                .consumerId("1")
                .consumerGroup("group_push")
                .bindRegistry("127.0.0.1:8848")
                .messageModel(MessageModel.CLUSTER)
                .registryType(RegistryType.NACOS)
                .startConsume(StartConsume.FROM_FIRST_OFFSET)
                .subscribe("topic1", "*")
                .messageListener(new CommonMessageListener() {
                    @Override
                    public boolean consume(List<ReadyMessage> messages) {
                        sum.addAndGet(messages.size());
                        for (ReadyMessage message : messages) {
                            int queueId = message.getQueueId();
                            String content = new String(message.getBody());
                            System.out.println("queueId: " + queueId + ", content: " + content +
                                    ", current is " + TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) +
                                    ", retry: " + message.getRetry());
                            System.out.println(message);
                        }
                        return true;
                    }
                }).build().start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println(sum.get());
        }));
        String asas  = new String("111");
//        ScheduledExecutorService timer = new ScheduledThreadPoolExecutor(1);
//        timer.scheduleAtFixedRate(() -> {
//            System.out.println(" ---- " + sum.get() + " ---- " );
//        }, 1 * 1000, 1 * 1000, TimeUnit.MILLISECONDS);
    }
}
