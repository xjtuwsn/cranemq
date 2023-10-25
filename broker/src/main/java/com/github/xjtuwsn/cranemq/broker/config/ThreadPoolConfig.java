package com.github.xjtuwsn.cranemq.broker.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @project:cranemq
 * @file:ThreadPoolConfig
 * @author:wsn
 * @create:2023/10/24-16:41
 */
@Configuration
public class ThreadPoolConfig {
    @Bean("producerMessageService")
    public ExecutorService producerMessageService() {
        return buildThreadPool(10, 20, 10000, "Producer Message Service");

    }

    @Bean("createTopicService")
    public ExecutorService createTopicService() {
        return buildThreadPool(5, 10, 5000, "Create Topic Service");
    }

    @Bean("simplePullService")
    public ExecutorService simplePullService() {
        return buildThreadPool(5, 10, 5000, "Simple Pull Service");
    }

    @Bean("handleHeartBeatService")
    public ExecutorService handleHeartBeatService() {
        return buildThreadPool(10, 20, 10000, "Handle HeartBeat Service");
    }

    @Bean("handlePullService")
    public ExecutorService handlePullService() {
        return buildThreadPool(12, 24, 10000, "Handle Pull Service");
    }

    @Bean("handleOffsetService")
    public ExecutorService handleOffsetService() {
        return buildThreadPool(8, 16, 6000, "Handle Offset Service");
    }

    @Bean("sendBackService")
    public ExecutorService sendBackService() {
        return buildThreadPool(7, 14, 5000, "SendBack Service");
    }

    private ExecutorService buildThreadPool(int coreSize, int maxSize, int capacity, String threadPoolName) {

        return new ThreadPoolExecutor(coreSize, maxSize, 60L, TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(capacity),
                threadFactory(threadPoolName),
                new ThreadPoolExecutor.AbortPolicy());
    }

    private ThreadFactory threadFactory(String name) {
        return new ThreadFactory() {
            AtomicInteger index = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, name + " NO." + index.getAndIncrement());
            }
        };
    }
}
