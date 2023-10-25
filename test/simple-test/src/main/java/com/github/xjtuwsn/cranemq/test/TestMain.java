package com.github.xjtuwsn.cranemq.test;

import com.github.xjtuwsn.cranemq.client.consumer.DefaultPushConsumer;
import com.github.xjtuwsn.cranemq.client.producer.MQSelector;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.github.xjtuwsn.cranemq.client.producer.DefaultMQProducer;
import com.github.xjtuwsn.cranemq.client.producer.balance.RoundRobinStrategy;
import com.github.xjtuwsn.cranemq.common.entity.Message;
import com.github.xjtuwsn.cranemq.common.remote.enums.RegistryType;

import java.util.List;

/**
 * @project:cranemq
 * @file:TestMain
 * @author:wsn
 * @create:2023/09/26-21:35
 */
public class TestMain {

    public static void main(String[] args) throws Exception {
        String topic = "topic1";
        Thread t = new Thread(() -> {
            DefaultMQProducer producer = new DefaultMQProducer("group1");
            producer.bindRegistry("127.0.0.1:11111", RegistryType.DEFAULT);
            producer.setLoadBalanceStrategy(new RoundRobinStrategy());
            producer.start();
            Message message1 = new Message(topic, "hhhh".getBytes());
            Message message2 = new Message(topic, "aaaa".getBytes());
            long start = System.nanoTime();
            for (int i = 0; i < 64; i++) {
                Message message = new Message(topic, ("" + i).getBytes());
                producer.send(message, new MQSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> queues, Object arg) {
//                        int index = (int) arg;
//                        return queues.get((index + 1) % queues.size());
                        return queues.get(0);
                    }
                }, i);
            }
            long end = System.nanoTime();
            double cost = (end - start) / 1e6;
            System.out.println("Single SYNC message cost " + cost + " ms totally");
//            producer.shutdown();
            DefaultPushConsumer defaultPushConsumer = new DefaultPushConsumer("12");
        });
        t.start();
    }
}
