package com.github.xjtuwsn.cranemq.broker.store.pool;

import com.github.xjtuwsn.cranemq.broker.store.PersistentConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @project:cranemq
 * @file:OutOfHeapMemoryPool
 * @author:wsn
 * @create:2023/10/04-10:17
 */
public class OutOfHeapMemoryPool {
    private static final Logger log = LoggerFactory.getLogger(OutOfHeapMemoryPool.class);

    private final int maxQueueSize;
    private final PersistentConfig persistentConfig;

    private LinkedBlockingQueue<ByteBuffer> queue;

    public OutOfHeapMemoryPool(PersistentConfig persistentConfig) {
        this.persistentConfig = persistentConfig;
        this.maxQueueSize = this.persistentConfig.getMaxOutOfMemoryPoolSize();
        this.queue = new LinkedBlockingQueue<>(this.maxQueueSize);
        log.info("New memory pool, size is {}", this.maxQueueSize);
    }

    public void init() {
        for (int i = 0; i < this.maxQueueSize; i++) {
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(this.persistentConfig.getCommitLogMaxSize());
            this.queue.offer(byteBuffer);
        }
        log.info("Finish memory pool initialize");
    }
    public void destory() {
        while (!this.queue.isEmpty()) {
            ByteBuffer byteBuffer = this.queue.poll();
            if (byteBuffer != null) {
                byteBuffer.clear();
            }
        }
        this.queue.clear();
    }
    public void returnMemory(ByteBuffer byteBuffer) {
        byteBuffer.position(0);
        byteBuffer.limit(this.persistentConfig.getCommitLogMaxSize());
        this.queue.offer(byteBuffer);
    }
    public ByteBuffer borrowMemmory() {
        ByteBuffer poll = this.queue.poll();
        if (poll == null) {
            log.warn("No enough direct memory");
        }
        log.info("Thread {} borrow a pool", Thread.currentThread().getName());
        return poll;
    }
    public int remainSize() {
        return this.queue.size();
    }
}
