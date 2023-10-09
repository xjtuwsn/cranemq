package com.github.xjtuwsn.cranemq.client.consumer.push;

import com.github.xjtuwsn.cranemq.common.entity.ReadyMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @project:cranemq
 * @file:BrokerQueueSnapShot
 * @author:wsn
 * @create:2023/10/08-17:11
 */
public class BrokerQueueSnapShot {

    private static final Logger log = LoggerFactory.getLogger(BrokerQueueSnapShot.class);

    private TreeMap<Long, ReadyMessage> messages = new TreeMap<>();

    private ReadWriteLock messageLock = new ReentrantReadWriteLock();

    private AtomicBoolean expired = new AtomicBoolean(false);

    public void putMessage(List<ReadyMessage> readyMessages) {
        try {
            messageLock.writeLock().lock();
            for (ReadyMessage readyMessage : readyMessages) {
                this.messages.put(readyMessage.getOffset(), readyMessage);
            }
        } catch (Exception e) {
            log.error("Put treemap occurs exception");
        } finally {
            messageLock.writeLock().unlock();
        }

    }
    public void removeMessages(List<ReadyMessage> messages) {

    }

    public void markExpired() {
        this.expired.set(true);
    }

    public boolean isExpired() {
        return this.expired.get();
    }
}
