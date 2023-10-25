package com.github.xjtuwsn.cranemq.client.consumer.push;

import com.github.xjtuwsn.cranemq.common.entity.Message;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @project:cranemq
 * @file:MessageQueueLock
 * @author:wsn
 * @create:2023/10/12-14:40
 * 本地消费队列锁
 */
public class MessageQueueLock {

    private static final Logger log = LoggerFactory.getLogger(MessageQueueLock.class);

    final private ConcurrentHashMap<MessageQueue, Object> lockTable = new ConcurrentHashMap<>();


    public void resetLock(List<MessageQueue> messageQueues) {
        lockTable.clear();
        for (MessageQueue messageQueue : messageQueues) {
            lockTable.put(messageQueue, new Object());
        }
    }

    public Object acquireLock(MessageQueue messageQueue) {
        Object o = new Object();
        Object prev = lockTable.putIfAbsent(messageQueue, o);
        return prev == null ? o : prev;
    }
}
