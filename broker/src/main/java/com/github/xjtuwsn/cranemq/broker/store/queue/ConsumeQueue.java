package com.github.xjtuwsn.cranemq.broker.store.queue;

import com.github.xjtuwsn.cranemq.broker.store.*;
import com.github.xjtuwsn.cranemq.broker.store.cmtlog.RecoveryListener;
import com.github.xjtuwsn.cranemq.broker.store.comm.AsyncRequest;
import com.github.xjtuwsn.cranemq.broker.store.comm.PutMessageResponse;
import com.github.xjtuwsn.cranemq.broker.store.comm.StoreRequestType;
import com.github.xjtuwsn.cranemq.broker.store.comm.StoreResponseType;
import com.github.xjtuwsn.cranemq.common.utils.BrokerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @project:cranemq
 * @file:ConsumerQueue
 * @author:wsn
 * @create:2023/10/04-21:29
 */
public class ConsumeQueue extends AbstractLinkedListOrganize implements GeneralStoreService {
    private static final Logger log = LoggerFactory.getLogger(ConsumeQueue.class);
    private int queueId;
    private String topic;
    private String fullPath;
    private PersistentConfig persistentConfig;
    private CreateRequestListener createListener;

    private RecoveryListener recoveryListener;
    // TODO 消费者队列实现
    public ConsumeQueue(int queueId, String topic, PersistentConfig persistentConfig) {
        this.queueId = queueId;
        this.topic = topic;
        this.persistentConfig = persistentConfig;
        this.fullPath = persistentConfig.getConsumerqueuePath() + topic + "\\" + queueId + "\\";
        this.init();
    }

    public PutMessageResponse updateQueueOffset(long offset, int size) {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile == null) {
            log.info("{}, {}", offset, size);
            mappedFile = this.createListener.onRequireCreate(topic, queueId, this.nextIndex());
            log.info("Create queue file success, mapped file is {}", mappedFile);
        }

        PutMessageResponse response = mappedFile.putOffsetIndex(offset, size);
        log.info("update queueoffset result: {}", response);
        if (response.getResponseType() == StoreResponseType.NO_ENOUGH_SPACE) {
            mappedFile = this.createListener.onRequireCreate(topic, queueId, this.nextIndex());
            response = mappedFile.putOffsetIndex(offset, size);
        }
        return response;
    }
    @Override
    public void start() {
        File queueRoot = new File(fullPath);
        File[] mapped = queueRoot.listFiles();
        Arrays.sort(mapped, Comparator.comparing(File::getName));
        int length = mapped.length;
        int index = 0;
        for (File mappedFiles : mapped) {
            MappedFile mappedFile = new MappedFile(index, persistentConfig.getMaxQueueSize(), mappedFiles.getName(),
                    fullPath + mappedFiles.getName(), persistentConfig);
            this.mappedTable.put(index, mappedFile);
            this.insertBeforeTail(mappedFile);

            if (index == length - 1) {
                this.binarySearchLastCommit(mappedFile);
            }

            index++;
        }
    }

    private void binarySearchLastCommit(MappedFile mappedFile) {
        ByteBuffer buffer = mappedFile.getWriteBuffer().slice();
        int cell = persistentConfig.getQueueUnit();
        int l = 0, r = persistentConfig.getMaxQueueItemNumber();
        while (l < r) {
            int mid = l + r >> 1;
            int pos = mid * cell;
            buffer.position(pos);
            if (buffer.getLong() == 0 && buffer.getInt() == 0) {
                r = mid - 1;
            } else {
                if (pos + cell >= buffer.capacity()) {
                    l = mid;
                    break;
                }
                buffer.position(pos + cell);
                if (buffer.getLong() == 0 && buffer.getInt() == 0) {
                    l = mid;
                    break;
                }
                l = mid + 1;
            }
        }
        int pos = l * cell;
        buffer.position(pos);
        long offset = buffer.getLong();
        int size = buffer.getInt();

        int newPos = pos + cell;
        mappedFile.setWritePointer(newPos);
        mappedFile.setCommitPointer(newPos);
        mappedFile.setFlushPointer(newPos);
        log.info("Consumequeue [topic: {}, queueId: {}, name: {}], recovery frome pos: {}",
                topic, queueId, mappedFile.getFileName(), newPos);
        this.recoveryListener.onUpdateOffset(offset, size);
    }
    public MappedFile getLastMappedFile() {
        MappedFile mappedFile = tail.prev;
        if (mappedFile == head) {
            return null;
        }
        return mappedFile;
    }

    public long currentLastOffset() {
        MappedFile lastMappedFile = getLastMappedFile();
        if (lastMappedFile == null) {
            return 0;
        }
        return lastMappedFile.getWrite() / persistentConfig.getQueueUnit()
                + (long) (this.mappedTable.size() - 3) * persistentConfig.getMaxQueueSize();
    }
    public boolean appendMappedFile(MappedFile mappedFile) {
        if (mappedFile == null) {
            return false;
        }
        this.insertBeforeTail(mappedFile);
        return true;
    }
    public void registerCreateListener(CreateRequestListener createListener) {
        this.createListener = createListener;
    }
    public void registerUpdateOffsetListener(RecoveryListener recoveryListener) {
        this.recoveryListener = recoveryListener;
    }
    @Override
    public void close() {
        MappedFile lastMappedFile = getLastMappedFile();
        if (lastMappedFile != null) {
            lastMappedFile.doFlush();
        }
    }

    @Override
    public String toString() {
        return "ConsumeQueue{" +
                "queueId=" + queueId +
                ", topic='" + topic + '\'' +
                '}';
    }
}
