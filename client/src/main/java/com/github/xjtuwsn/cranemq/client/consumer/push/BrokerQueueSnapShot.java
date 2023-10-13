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

    private AtomicBoolean locked = new AtomicBoolean(false);

    private long lastLockTime = System.currentTimeMillis();

    private volatile long maxOffset = 0L;

    public void putMessage(List<ReadyMessage> readyMessages) {
        try {
            messageLock.writeLock().lock();
            for (ReadyMessage readyMessage : readyMessages) {
                this.messages.put(readyMessage.getOffset(), readyMessage);
                maxOffset = readyMessage.getOffset();
            }
        } catch (Exception e) {
            log.error("Put treemap occurs exception");
        } finally {
            messageLock.writeLock().unlock();
        }

    }
    public long removeMessages(List<ReadyMessage> messages) {
        long result = -1L;
        try {
            messageLock.writeLock().lock();
            result = maxOffset + 1;
            for (ReadyMessage readyMessage : messages) {
                long offset = readyMessage.getOffset();
                this.messages.remove(offset);
            }
            if (!this.messages.isEmpty()) {
                result = this.messages.firstKey();
            }
        } catch (Exception e) {
            log.error("Remove message from treemap error");
        } finally {
            messageLock.writeLock().unlock();
        }

        return result;
    }

    public void markExpired() {
        this.expired.set(true);
    }

    public boolean isExpired() {
        return this.expired.get();
    }
    public void renewLockTime() {
        this.lastLockTime = System.currentTimeMillis();
    }

    public boolean tryLock() {
        long start = System.currentTimeMillis();
        while (!this.locked.compareAndSet(false, true)) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                log.error("CAS has been interrupted");
            }
            long now = System.currentTimeMillis();
            if (now - start > 1000) {
                return false;
            }
        }
        return true;
    }

    public boolean releaseLock() {
        return this.locked.compareAndSet(true, false);
    }
}
