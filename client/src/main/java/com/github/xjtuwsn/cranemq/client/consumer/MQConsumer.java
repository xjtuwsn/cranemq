package com.github.xjtuwsn.cranemq.client.consumer;

/**
 * @project:cranemq
 * @file:MQConsumer
 * @author:wsn
 * @create:2023/09/27-11:11
 */
public interface MQConsumer {
    void subscribe(String topic, String tags);
    void bindRegistry(String address);
    void start();

    void shutdown();
}
