package com.github.xjtuwsn.cranemq.broker.client;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import org.checkerframework.checker.index.qual.NonNegative;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @project:cranemq
 * @file:ClientLockMananger
 * @author:wsn
 * @create:2023/10/12-17:24
 */
// TODO 添加消费者申请锁的各个逻辑，检查在第一次访问队列时，根据offset判断释放本地队列锁的逻辑是否争取
public class ClientLockMananger {

    private static final Logger log = LoggerFactory.getLogger(ClientLockMananger.class);
    // group: [messagequeue: lock]
    private ConcurrentHashMap<String, Cache<MessageQueue, QueueLock>> lockTable = new ConcurrentHashMap<>();

    private ReentrantLock lock = new ReentrantLock();

    public boolean applyLock(String group, MessageQueue messageQueue, String clientId) {
        try {
            lock.lock();
            Cache<MessageQueue, QueueLock> cache = lockTable.get(group);
            if (cache == null) {
                cache = Caffeine.newBuilder()
                        .maximumSize(3000)
                        .expireAfter(new Expiry<MessageQueue, QueueLock>() {
                            @Override
                            public long expireAfterCreate(MessageQueue messageQueue, QueueLock queueLock, long l) {
                                return TimeUnit.SECONDS.toNanos(1);
                            }

                            @Override
                            public long expireAfterUpdate(MessageQueue messageQueue, QueueLock queueLock, long l, @NonNegative long l1) {
                                return TimeUnit.SECONDS.toNanos(30) + l1;
                            }

                            @Override
                            public long expireAfterRead(MessageQueue messageQueue, QueueLock queueLock, long l, @NonNegative long l1) {
                                return l1;
                            }
                        }).build();
                lockTable.put(group, cache);
            }
            QueueLock queueLock = cache.getIfPresent(messageQueue);
            if (queueLock != null && !queueLock.isMine(clientId)) {
                return false;
            }
            if (queueLock == null) {
                cache.put(messageQueue, new QueueLock(clientId));
            }
            log.info("Client {}, group {} acquire the lock", clientId, group);
            return true;
        } catch (Exception e) {
            log.error("Execpiton when apply for lock");
        } finally {
            lock.unlock();
        }
        return false;
    }

    public boolean renewLock(String group, MessageQueue messageQueue, String clientId) {
        try {
            lock.lock();
            Cache<MessageQueue, QueueLock> cache = lockTable.get(group);
            if (cache == null) {
                return false;
            }
            QueueLock queueLock = cache.getIfPresent(messageQueue);
            if (queueLock == null || !queueLock.isMine(clientId)) {
                return false;
            }
            queueLock.renew();
            cache.put(messageQueue, queueLock);
            return true;
        } catch (Exception e) {
            log.error("Execpiton when renew for lock");
        } finally {
            lock.unlock();
        }
        return false;
    }

    public boolean releaseLock(String group, MessageQueue messageQueue, String clientId) {
        try {
            lock.lock();
            Cache<MessageQueue, QueueLock> cache = lockTable.get(group);
            if (cache == null) {
                return true;
            }
            QueueLock queueLock = cache.getIfPresent(messageQueue);
            if (queueLock == null) {
                return true;
            }
            if (!queueLock.isMine(clientId)) {
                return false;
            }
            cache.invalidate(messageQueue);
            return true;
        } catch (Exception e) {
            log.error("Execpiton when release a lock");
        } finally {
            lock.unlock();
        }
        return false;
    }
    class QueueLock {
        private String ownerId;
        private long applyTime;

        public QueueLock(String ownerId) {
            this.ownerId = ownerId;
            this.applyTime = System.currentTimeMillis();
        }

        public boolean isMine(String clientId) {
            return ownerId.equals(clientId);
        }
        public void renew() {
            this.applyTime = System.currentTimeMillis();
        }
    }
}
