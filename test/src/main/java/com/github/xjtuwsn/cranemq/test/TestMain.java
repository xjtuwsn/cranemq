package com.github.xjtuwsn.cranemq.test;

import com.github.xjtuwsn.cranemq.broker.core.MqBroker;
import com.github.xjtuwsn.cranemq.client.producer.MQSelector;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.github.xjtuwsn.cranemq.common.net.RemoteHook;
import com.github.xjtuwsn.cranemq.client.producer.DefaultMQProducer;
import com.github.xjtuwsn.cranemq.client.producer.balance.RoundRobinStrategy;
import com.github.xjtuwsn.cranemq.common.entity.Message;
import com.github.xjtuwsn.cranemq.common.net.RemoteAddress;
import com.github.xjtuwsn.cranemq.registry.core.Registry;
import org.junit.Test;

import java.util.List;

/**
 * @project:cranemq
 * @file:TestMain
 * @author:wsn
 * @create:2023/09/26-21:35
 */
public class TestMain {

    public static void main(String[] args) throws Exception {
//        MqBroker broker = new MqBroker();
//        broker.start();
        String topic = "topic1";
        RemoteHook hook = new RemoteHook() {
            @Override
            public void beforeMessage() {
               // System.out.println("message before");
            }

            @Override
            public void afterMessage() {
               // System.out.println("response come");
            }
        };
        Thread t = new Thread(() -> {

            RemoteAddress remoteAddress = new RemoteAddress("127.0.0.1", 9999);
            DefaultMQProducer producer = new DefaultMQProducer("group1", hook);
            // producer.setBrokerAddress(remoteAddress);
            producer.bindRegistery("127.0.0.1:11111");
            producer.setLoadBalanceStrategy(new RoundRobinStrategy());
            producer.start();
            Message message1 = new Message(topic, "hhhh".getBytes());
            Message message2 = new Message(topic, "aaaa".getBytes());
            // producer.send(message2);
            long start = System.nanoTime();
            for (int i = 0; i < 5; i++) {

                producer.send(message2, new MQSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> queues, Object arg) {
                        int index = (int) arg;
                        return queues.get((index + 1) % queues.size());
                    }
                }, i);
            }
            long end = System.nanoTime();
            double cost = (end - start) / 1e6;
            System.out.println("Single SYNC message cost " + cost + " ms totally");
//            producer.shutdown();
        });
        t.start();
    }
    @Test
    public void test() {
        int x = 10;
        System.out.println(x & (-x));
    }
}
