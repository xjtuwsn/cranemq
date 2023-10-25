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
        ThreadPoolExecutor executorService = new ThreadPoolExecutor(10, 20,
                60L, TimeUnit.SECONDS, new LinkedBlockingDeque<>(10000),
                threadFactory("producerMessageService"));
        return executorService;
    }

    public ThreadFactory threadFactory(String name) {
        return new ThreadFactory() {
            AtomicInteger index = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, name + " NO." + index.getAndIncrement());
            }
        };
    }
}
