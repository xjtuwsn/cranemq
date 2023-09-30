package com.github.xjtuwsn.cranemq.test;

import com.github.xjtuwsn.cranemq.broker.core.MqBroker;
import com.github.xjtuwsn.cranemq.client.hook.RemoteHook;
import com.github.xjtuwsn.cranemq.client.hook.SendCallback;
import com.github.xjtuwsn.cranemq.client.producer.DefaultMQProducer;
import com.github.xjtuwsn.cranemq.client.producer.balance.RoundRobinStrategy;
import com.github.xjtuwsn.cranemq.client.producer.result.SendResult;
import com.github.xjtuwsn.cranemq.common.entity.Message;
import com.github.xjtuwsn.cranemq.common.net.RemoteAddress;
import com.github.xjtuwsn.cranemq.registry.core.Registry;

import java.util.Arrays;

/**
 * @project:cranemq
 * @file:TestMain
 * @author:wsn
 * @create:2023/09/26-21:35
 */
public class TestMain {

    public static void main(String[] args) {
        MqBroker broker = new MqBroker();
        broker.start();
        Registry registry = new Registry();
        registry.start();
        String topic = "topic3";
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
//            producer.send(message1, new SendCallback() {
//                @Override
//                public void onSuccess(SendResult result) {
//                    System.out.println("-----------------------");
//                    System.out.println("success, " + result);
//                    System.out.println("-----------------------");
//                }
//
//                @Override
//                public void onFailure(Throwable reason) {
//                    System.out.println("-----------------------");
//                    System.out.println("Failure, " + reason);
//                    System.out.println("-----------------------");
//                }
//            });
//            try {
//                Thread.sleep(200);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
            long start = System.nanoTime();
            for (int i = 0; i < 5000; i++) {

                producer.send(message2);

            }
            long end = System.nanoTime();
            double cost = (end - start) / 1e6;
            System.out.println("Single SYNC message cost " + cost + " ms totally");
//            producer.shutdown();
        });
        t.start();
    }
}
