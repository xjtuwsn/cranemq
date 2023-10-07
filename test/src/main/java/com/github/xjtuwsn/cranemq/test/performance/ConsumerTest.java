package com.github.xjtuwsn.cranemq.test.performance;

import com.github.xjtuwsn.cranemq.client.consumer.DefaultPullConsumer;

/**
 * @project:cranemq
 * @file:ConsumerTest
 * @author:wsn
 * @create:2023/10/07-16:47
 */
public class ConsumerTest {
    public static void main(String[] args) throws InterruptedException {
        DefaultPullConsumer defaultPullConsumer = new DefaultPullConsumer("groupa");
        defaultPullConsumer.bindRegistry("127.0.0.1:11111");
        defaultPullConsumer.subscribe("topic1", "*");
        defaultPullConsumer.start();
        System.out.println(defaultPullConsumer.listQueues());
    }
}
