package com.github.xjtuwsn.cranemq.broker.client;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import org.checkerframework.checker.index.qual.NonNegative;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @project:cranemq
 * @file:ClientLockMananger
 * @author:wsn
 * @create:2023/10/12-17:24
 * 队列锁管理
 */
public class ClientLockManager {

    private static final Logger log = LoggerFactory.getLogger(ClientLockManager.class);
    // group: [messageQueue: lock]
    private ConcurrentHashMap<String, Cache<MessageQueue, QueueLock>> lockTable = new ConcurrentHashMap<>();

    private ReentrantLock lock = new ReentrantLock();

    /**
     * 分发队列锁
     * @param group
     * @param messageQueue
     * @param clientId
     * @return
     */
    public boolean applyLock(String group, MessageQueue messageQueue, String clientId) {
        try {
            lock.lock();
            Cache<MessageQueue, QueueLock> cache = lockTable.get(group);
            // 如果不存在就新建，并置更新续期和到期时间
            if (cache == null) {
                cache = Caffeine.newBuilder()
                        .maximumSize(3000)
                        .expireAfter(new Expiry<MessageQueue, QueueLock>() {
                            @Override
                            public long expireAfterCreate(MessageQueue messageQueue, QueueLock queueLock, long l) {
                                return TimeUnit.MINUTES.toNanos(1);
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
            log.info("Client {}, group {} acquire the lock, queue {}", clientId, group, messageQueue);
            return true;
        } catch (Exception e) {
            log.error("Exception when apply for lock");
        } finally {
            lock.unlock();
        }
        return false;
    }

    /**
     * 续期锁
     * @param group
     * @param messageQueue
     * @param clientId
     * @return
     */
    public boolean renewLock(String group, MessageQueue messageQueue, String clientId) {
        try {
            lock.lock();
            Cache<MessageQueue, QueueLock> cache = lockTable.get(group);
            if (cache == null) {
                return false;
            }
            QueueLock queueLock = cache.getIfPresent(new MessageQueue(messageQueue.getTopic(), messageQueue.getBrokerName(),
                    messageQueue.getQueueId()));
            if (queueLock == null || !queueLock.isMine(clientId)) {
                return false;
            }
            queueLock.renew();
            cache.put(messageQueue, queueLock);
            log.info("Client {}, group {} renew the lock, queue {}", clientId, group, messageQueue);
            return true;
        } catch (Exception e) {
            log.error("Exception when renew for lock");
        } finally {
            lock.unlock();
        }
        return false;
    }

    /**
     * 释放客户端所持有的所有锁
     * @param group
     * @param clientId
     */
    public void releaseLock(String group, String clientId) {
        try {
            lock.lock();
            Cache<MessageQueue, QueueLock> cache = lockTable.get(group);
            if (cache == null) {
                return;
            }
            ConcurrentMap<MessageQueue, QueueLock> map = cache.asMap();

            for (Map.Entry<MessageQueue, QueueLock> entry : map.entrySet()) {
                MessageQueue messageQueue = entry.getKey();
                QueueLock queueLock = entry.getValue();
                if (queueLock.isMine(clientId)) {
                    cache.invalidate(messageQueue);
                }
            }

        } catch (Exception e) {
            log.error("Exception when release lock");
        } finally {
            lock.unlock();
        }
    }

    /**
     * 释放某个队列的锁
     * @param group
     * @param messageQueue
     * @param clientId
     * @return
     */
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
            log.info("Client {}, group {} release the lock, queue {}", clientId, group, messageQueue);
            return true;
        } catch (Exception e) {
            log.error("Exception when release a lock");
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

        @Override
        public String toString() {
            return "QueueLock{" +
                    "ownerId='" + ownerId + '\'' +
                    ", applyTime=" + applyTime +
                    '}';
        }
    }
}
