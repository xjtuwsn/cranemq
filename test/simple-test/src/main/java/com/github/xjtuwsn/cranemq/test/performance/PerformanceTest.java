package com.github.xjtuwsn.cranemq.test.performance;

import com.github.xjtuwsn.cranemq.client.producer.DefaultMQProducer;
import com.github.xjtuwsn.cranemq.client.producer.balance.RoundRobinStrategy;
import com.github.xjtuwsn.cranemq.common.entity.Message;
import com.github.xjtuwsn.cranemq.common.remote.enums.RegistryType;

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
        int threadNum = 10, loop = 20000;
        CountDownLatch latch = new CountDownLatch(threadNum);
        ExecutorService threadPool = new ThreadPoolExecutor(11,
                22, 60L, TimeUnit.SECONDS, new LinkedBlockingDeque<>(1000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r);
                    }
                }, new ThreadPoolExecutor.AbortPolicy());
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        producer.bindRegistry("127.0.0.1:11111", RegistryType.DEFAULT);
        producer.setLoadBalanceStrategy(new RoundRobinStrategy());
        producer.start();
        byte[] data = new byte[1024];
        Message message1 = new Message(topic, data);
        producer.send(message1);
        long start = System.nanoTime();
        for (int i = 0; i < threadNum; i++) {
            threadPool.execute(() -> {
                for (int j = 0; j < loop; j++) {
                    producer.send(message1);
                }
                latch.countDown();
            });
        }
        latch.await();
        long end = System.nanoTime();
        double cost = (end - start) / 1e6;
        FileWriter fw = new FileWriter(new File("D:\\code\\opensource\\cranemq\\test\\simple-test\\src\\main\\resources\\test.txt"), true);
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
