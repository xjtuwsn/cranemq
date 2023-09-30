package com.github.xjtuwsn.cranemq.client.producer.balance;

import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.github.xjtuwsn.cranemq.common.exception.CraneClientException;
import com.github.xjtuwsn.cranemq.common.route.BrokerData;
import com.github.xjtuwsn.cranemq.common.route.TopicRouteInfo;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @project:cranemq
 * @file:RoundRobinStrategy
 * @author:wsn
 * @create:2023/09/30-16:36
 */
public class RoundRobinStrategy implements LoadBalanceStrategy {
    private ConcurrentHashMap<String, AtomicInteger> globalIndex = new ConcurrentHashMap<>();
    @Override
    public MessageQueue getNextQueue(String topic, TopicRouteInfo info) throws CraneClientException {
        int total = info.getTotalWriteQueueNumber();
        AtomicInteger atomicInteger = this.globalIndex.get(topic);
        if (atomicInteger == null) {
            atomicInteger = new AtomicInteger(0);
            this.globalIndex.put(topic, atomicInteger);
        }
        int cur = atomicInteger.getAndIncrement();
        int next = cur % total;
        MessageQueue queue = info.getNumberKQueue(next);
        queue.setTopic(topic);
        return queue;
    }
}
