package com.github.xjtuwsn.cranemq.broker.store;

import com.github.xjtuwsn.cranemq.broker.BrokerController;
import com.github.xjtuwsn.cranemq.broker.store.cmtlog.CommitLog;
import com.github.xjtuwsn.cranemq.broker.store.cmtlog.RecoveryListener;
import com.github.xjtuwsn.cranemq.broker.store.comm.PutMessageResponse;
import com.github.xjtuwsn.cranemq.broker.store.comm.StoreResponseType;
import com.github.xjtuwsn.cranemq.broker.store.flush.AsyncFlushDiskService;
import com.github.xjtuwsn.cranemq.broker.store.flush.FlushDiskService;
import com.github.xjtuwsn.cranemq.broker.store.flush.SyncFlushDiskService;
import com.github.xjtuwsn.cranemq.broker.store.queue.ConsumeQueueManager;
import com.github.xjtuwsn.cranemq.common.config.FlushDisk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @project:cranemq
 * @file:StoreService
 * @author:wsn
 * @create:2023/10/03-16:35
 */
public class MessageStoreCenter implements GeneralStoreService {
    private static final Logger log = LoggerFactory.getLogger(MessageStoreCenter.class);
    private BrokerController brokerController;
    private CommitLog commitLog;
    private ConsumeQueueManager consumeQueueManager;
    private FlushDiskService flushDiskService;

    public MessageStoreCenter(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.commitLog = new CommitLog(this.brokerController);
        this.consumeQueueManager = new ConsumeQueueManager(this.brokerController,
                this.brokerController.getPersistentConfig());
        if (brokerController.getPersistentConfig().getFlushDisk() == FlushDisk.ASYNC) {
            this.flushDiskService = new AsyncFlushDiskService(brokerController.getPersistentConfig(), commitLog,
                    consumeQueueManager);
        } else {
            this.flushDiskService = new SyncFlushDiskService();
        }
    }
    public PutMessageResponse putMessage(StoreInnerMessage innerMessage) {
        if (innerMessage == null) {
            log.error("Null put message request");
        }
        long start = System.nanoTime();
        PutMessageResponse response = this.commitLog.writeMessage(innerMessage);
        long end1 = System.nanoTime();

        if (response.getResponseType() == StoreResponseType.PARAMETER_ERROR) {
            log.error("Put message to CommitLog error");

        }
        PutMessageResponse putOffsetResp = null;
        if (response.getResponseType() == StoreResponseType.STORE_OK) {
            long offset = response.getOffset();
            int size = response.getSize();
            putOffsetResp = this.consumeQueueManager.updateOffset(offset, innerMessage.getTopic(),
                    innerMessage.getQueueId(), size);
        }
        if (putOffsetResp == null) {
            return new PutMessageResponse(StoreResponseType.PARAMETER_ERROR);
        }
        if (putOffsetResp.getResponseType() == StoreResponseType.STORE_OK) {
            if (brokerController.getPersistentConfig().getFlushDisk() == FlushDisk.SYNC) {
                this.flushDiskService.flush(response.getMappedFile());
                this.flushDiskService.flush(putOffsetResp.getMappedFile());
            }
        }
        return putOffsetResp;

    }
    public void start() {
        this.consumeQueueManager.registerRecoveryListener(new RecoveryListener() {
            @Override
            public void onUpdateOffset(long offset, int size) {
                commitLog.recoveryFromQueue(offset, size);
            }
        });
        this.consumeQueueManager.start();
        this.commitLog.start();
        if (this.flushDiskService instanceof AsyncFlushDiskService) {
            ((AsyncFlushDiskService) flushDiskService).start();
            log.info("Async flush disk service start successfully");
        }
    }

    @Override
    public void close() {

    }
}
