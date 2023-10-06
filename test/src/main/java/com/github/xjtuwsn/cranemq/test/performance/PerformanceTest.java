package com.github.xjtuwsn.cranemq.test.performance;

import com.github.xjtuwsn.cranemq.broker.core.MqBroker;
import com.github.xjtuwsn.cranemq.common.net.RemoteHook;
import com.github.xjtuwsn.cranemq.client.producer.DefaultMQProducer;
import com.github.xjtuwsn.cranemq.client.producer.balance.RoundRobinStrategy;
import com.github.xjtuwsn.cranemq.common.entity.Message;
import com.github.xjtuwsn.cranemq.registry.core.Registry;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.*;

/**
 * @project:cranemq
 * @file:PerformanceTest
 * @author:wsn
 * @create:2023/09/30-19:12
 */
public class PerformanceTest {
    public static void main(String[] args) throws InterruptedException, IOException {
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
        int threadNum = 5, loop = 1;
        CountDownLatch latch = new CountDownLatch(threadNum);
        ExecutorService threadPool = new ThreadPoolExecutor(10,
                22, 60L, TimeUnit.SECONDS, new LinkedBlockingDeque<>(1000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r);
                    }
                }, new ThreadPoolExecutor.AbortPolicy());
        DefaultMQProducer producer = new DefaultMQProducer("group1", hook);
        producer.bindRegistery("127.0.0.1:11111");
        producer.setLoadBalanceStrategy(new RoundRobinStrategy());
        producer.start();

        Message message1 = new Message(topic, "hhhh".getBytes());
        producer.send(message1);
        long start = System.nanoTime();
        for (int i = 0; i < threadNum; i++) {
            threadPool.execute(() -> {
                for (int j = 0; j < loop; j++) {
                    producer.send(message1);
                }
                System.out.println(222);
                latch.countDown();
            });
        }
        latch.await();
        long end = System.nanoTime();
        double cost = (end - start) / 1e6;
        FileWriter fw = new FileWriter(new File("D:\\code\\opensource\\cranemq\\test\\src\\main\\resources\\test.txt"), true);
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(String.valueOf(cost));
        bw.write("\n");
        bw.flush();
        System.out.println("--------------------------------------------------------------------------------------------");
        System.out.println("Single SYNC message cost " + cost + " ms totally");
    }
    public static void start() {

    }
}
