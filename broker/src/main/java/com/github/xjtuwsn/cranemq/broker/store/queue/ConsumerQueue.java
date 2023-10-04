package com.github.xjtuwsn.cranemq.broker.store.queue;

import com.github.xjtuwsn.cranemq.broker.store.PersistentConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @project:cranemq
 * @file:ConsumerQueue
 * @author:wsn
 * @create:2023/10/04-21:29
 */
public class ConsumerQueue {
    private static final Logger log = LoggerFactory.getLogger(ConsumerQueue.class);
    private ConcurrentHashMap<Integer, ConsumerQueueInner> innerTable = new  ConcurrentHashMap<>();
    private String topic;
    private int headIndex = -1;
    private int tailIndex = -2;
    private PersistentConfig persistentConfig;
    private ConsumerQueueInner head, tail;
    // TODO 消费者队列实现
    public ConsumerQueue() {
        this.head = new ConsumerQueueInner(headIndex);
        this.tail = new ConsumerQueueInner(tailIndex);
        this.head.next = this.tail;
        this.tail.prev = this.head;

    }


}
