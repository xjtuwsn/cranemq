package com.github.xjtuwsn.cranemq.client.consumer;

import com.github.xjtuwsn.cranemq.common.remote.enums.RegistryType;

/**
 * @project:cranemq
 * @file:MQConsumer
 * @author:wsn
 * @create:2023/09/27-11:11
 */
public interface MQConsumer {
    void setId(String id);
    void subscribe(String topic, String tags);
    void bindRegistry(String address, RegistryType registryType);
    void start();

    void shutdown();
}
