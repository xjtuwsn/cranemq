package com.github.xjtuwsn.cranemq.test.performance;

import com.github.xjtuwsn.cranemq.client.consumer.DefaultPullConsumer;
import com.github.xjtuwsn.cranemq.client.consumer.PullResult;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.github.xjtuwsn.cranemq.common.remote.enums.RegistryType;

import java.util.List;

/**
 * @project:cranemq
 * @file:ConsumerTest
 * @author:wsn
 * @create:2023/10/07-16:47
 */
public class ConsumerTest {
    public static void main(String[] args) throws InterruptedException {
        DefaultPullConsumer defaultPullConsumer = new DefaultPullConsumer("groupa");
        defaultPullConsumer.bindRegistry("127.0.0.1:11111", RegistryType.DEFAULT);
        defaultPullConsumer.subscribe("topic1", "*");
        defaultPullConsumer.start();
        List<MessageQueue> list = defaultPullConsumer.listQueues();
         PullResult pull = defaultPullConsumer.pull(list.get(0), 100, 3);
         System.out.println(pull);
    }
}
