package com.github.xjtuwsn.cranemq.broker.store.flush;

import com.github.xjtuwsn.cranemq.broker.store.AbstractLinkedListOrganize;
import com.github.xjtuwsn.cranemq.broker.store.MappedFile;
import com.github.xjtuwsn.cranemq.broker.store.PersistentConfig;
import com.github.xjtuwsn.cranemq.broker.store.cmtlog.CommitLog;
import com.github.xjtuwsn.cranemq.broker.store.queue.ConsumeQueue;
import com.github.xjtuwsn.cranemq.broker.store.queue.ConsumeQueueManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @project:cranemq
 * @file:AsyncFlushDiskService
 * @author:wsn
 * @create:2023/10/06-11:04
 * 异步刷盘服务
 */
public class AsyncFlushDiskService extends Thread implements FlushDiskService {
    private static final Logger log = LoggerFactory.getLogger(AsyncFlushDiskService.class);

    private boolean isStop = false;
    private PersistentConfig persistentConfig;
    private CommitLog commitLog;
    private ConsumeQueueManager consumeQueueManager;
    public AsyncFlushDiskService(PersistentConfig persistentConfig, CommitLog commitLog,
                                 ConsumeQueueManager consumeQueueManager) {
        this.persistentConfig = persistentConfig;
        this.commitLog = commitLog;
        this.consumeQueueManager = consumeQueueManager;
    }
    @Override
    public void run() {
        while (!isStop) {
            // 定时进行刷盘
            flush();
            try {
                Thread.sleep(persistentConfig.getFlushDiskInterval());
            } catch (InterruptedException e) {
                log.warn("Flush disk thread has been Interrupted");
            }
        }
    }

    /**
     * 执行刷盘
     */
    @Override
    public void flush() {
        // 进行commitLog的刷盘
        Iterator<MappedFile> commitLogIterator = commitLog.iterator();
        while (commitLogIterator.hasNext()) {
            MappedFile next = commitLogIterator.next();
            if (next != null) {
                next.doFlush();
            }
        }
        // 进行所有消费队列的刷盘
        Iterator<ConcurrentHashMap<Integer, ConsumeQueue>> allQueue = consumeQueueManager.iterator();
        while (allQueue.hasNext()) {
            ConcurrentHashMap<Integer, ConsumeQueue> next = allQueue.next();
            Collection<ConsumeQueue> values = next.values();
            for (ConsumeQueue queue : values) {
                Iterator<MappedFile> queueIterator = queue.iterator();
                while (queueIterator.hasNext()) {
                    MappedFile next1 = queueIterator.next();
                    next1.doFlush();
                }
            }
        }
    }

    @Override
    public void flush(MappedFile mappedFile) {

    }

    public void setStop() {
        isStop = true;
    }
}
