package com.github.xjtuwsn.cranemq.example;

import com.github.xjtuwsn.cranemq.client.hook.SendCallback;
import com.github.xjtuwsn.cranemq.client.producer.DefaultMQProducer;
import com.github.xjtuwsn.cranemq.client.producer.MQSelector;
import com.github.xjtuwsn.cranemq.client.producer.balance.RandomStrategy;
import com.github.xjtuwsn.cranemq.client.producer.balance.RoundRobinStrategy;
import com.github.xjtuwsn.cranemq.client.producer.result.SendResult;
import com.github.xjtuwsn.cranemq.common.entity.Message;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.github.xjtuwsn.cranemq.common.remote.enums.RegistryType;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @project:cranemq
 * @file:Producer
 * @author:wsn
 * @create:2023/10/25-22:04
 */
public class Producer {
    private static DefaultMQProducer producer;

    private static String topic = "testTopic";

    private static String tag = "";

    private static String content = "Hello MQ";
    public static void main(String[] args) {

        producer = new DefaultMQProducer("test_group");
        producer.bindRegistry("127.0.0.1:8848", RegistryType.NACOS);

//        producer.bindRegistry("127.0.0.1:11111", RegistryType.DEFAULT);
//        producer.bindRegistry("127.0.0.1:2181", RegistryType.ZOOKEEPER);

        producer.setLoadBalanceStrategy(new RoundRobinStrategy());

//        producer.setLoadBalanceStrategy(new RandomStrategy());

        producer.start();

        sendMessage();

        producer.shutdown();
    }

    public static void sendMessage() {
        // SYNC + SINGLE
        Message message = new Message(topic, tag, content.getBytes());
        SendResult result1 = producer.send(message);
        System.out.println(result1);

        // SYNC + BATCH
        Message message2 = new Message(topic, tag, (content + content).getBytes());
        SendResult result2 = producer.send(Arrays.asList(message, message2));
        System.out.println(result2);

        // ASYNC
        producer.send(message, new SendCallback() {
            @Override
            public void onSuccess(SendResult result) {
                System.out.println("Success send");
            }

            @Override
            public void onFailure(Throwable reason) {
                System.out.println("Failed");
            }
        });

        // ONE WAY
        producer.send(message, true);

        // DELAY
        producer.send(message, 10, TimeUnit.SECONDS);

        // ORDERED
        for (int i = 0; i < 10; i++) {
            producer.send(message, new MQSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> queues, Object arg) {
                    int len = queues.size();
                    int v = (int) arg;
                    return queues.get(v % len);
                }
            }, i);
        }
    }
}
