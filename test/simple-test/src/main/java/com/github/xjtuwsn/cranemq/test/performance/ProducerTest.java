package com.github.xjtuwsn.cranemq.test.performance;

import com.github.xjtuwsn.cranemq.client.producer.balance.RoundRobinStrategy;
import com.github.xjtuwsn.cranemq.common.remote.RemoteHook;
import com.github.xjtuwsn.cranemq.client.producer.DefaultMQProducer;
import com.github.xjtuwsn.cranemq.common.entity.Message;
import com.github.xjtuwsn.cranemq.common.remote.RemoteAddress;
import com.github.xjtuwsn.cranemq.common.remote.enums.RegistryType;

import java.util.concurrent.*;

/**
 * @project:cranemq
 * @file:ProducerTest
 * @author:wsn
 * @create:2023/09/28-16:18
 */
public class ProducerTest {
    public static void main(String[] args) {
        ExecutorService service = new ThreadPoolExecutor(11,
                32,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(1000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r);
                    }
                },
                new ThreadPoolExecutor.AbortPolicy());
        int thread = 10, number = 200000;

        DefaultMQProducer producer = new DefaultMQProducer("group_push");
        producer.bindRegistery("127.0.0.1:11111", RegistryType.DEFAULT);
        producer.setLoadBalanceStrategy(new RoundRobinStrategy());
        producer.start();
        byte[] data = new byte[5 * 1024];
        Message message1 = new Message("topic1", data);
        for (int i = 0; i < thread; i++) {
            int cur = i;
            service.execute(() -> {

                for (int j = 0; j < number; j++) {

                    producer.send(message1);
                }

            });
        }

        while (!service.isTerminated()) {

        }
        producer.shutdown();
    }
}
