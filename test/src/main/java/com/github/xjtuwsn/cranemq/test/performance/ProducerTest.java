package com.github.xjtuwsn.cranemq.test.performance;

import com.github.xjtuwsn.cranemq.client.hook.RemoteHook;
import com.github.xjtuwsn.cranemq.client.producer.DefaultMQProducer;
import com.github.xjtuwsn.cranemq.common.entity.Message;
import com.github.xjtuwsn.cranemq.common.net.RemoteAddress;

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
                new LinkedBlockingDeque<>(100),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r);
                    }
                },
                new ThreadPoolExecutor.AbortPolicy());
        int thread = 16, number = 100;
        RemoteAddress remoteAddress = new RemoteAddress("127.0.0.1", 9999);
        DefaultMQProducer producer = new DefaultMQProducer("group1", new RemoteHook() {
            @Override
            public void beforeMessage() {
                System.out.println(111);
            }

            @Override
            public void afterMessage() {
                System.out.println("response come");
            }
        });

        producer.setBrokerAddress(remoteAddress);
        producer.start();
        for (int i = 0; i < thread; i++) {
            int cur = i;
            service.execute(() -> {

                for (int j = 0; j < number; j++) {
                    Message message1 = new Message("topic1", (cur + "-" + j).getBytes());
                    producer.send(message1);
                }

            });
        }

        while (!service.isTerminated()) {

        }
        producer.shutdown();
    }
}
