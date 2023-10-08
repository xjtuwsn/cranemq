package com.github.xjtuwsn.cranemq.broker.store;

import cn.hutool.core.lang.Pair;
import com.github.xjtuwsn.cranemq.broker.BrokerController;
import com.github.xjtuwsn.cranemq.broker.store.cmtlog.CommitLog;
import com.github.xjtuwsn.cranemq.broker.store.cmtlog.RecoveryListener;
import com.github.xjtuwsn.cranemq.broker.store.comm.PutMessageResponse;
import com.github.xjtuwsn.cranemq.broker.store.comm.StoreInnerMessage;
import com.github.xjtuwsn.cranemq.broker.store.comm.StoreResponseType;
import com.github.xjtuwsn.cranemq.broker.store.flush.AsyncFlushDiskService;
import com.github.xjtuwsn.cranemq.broker.store.flush.FlushDiskService;
import com.github.xjtuwsn.cranemq.broker.store.flush.SyncFlushDiskService;
import com.github.xjtuwsn.cranemq.broker.store.queue.ConsumeQueue;
import com.github.xjtuwsn.cranemq.broker.store.queue.ConsumeQueueManager;
import com.github.xjtuwsn.cranemq.common.command.payloads.req.MQCreateTopicRequest;
import com.github.xjtuwsn.cranemq.common.command.payloads.req.MQSimplePullRequest;
import com.github.xjtuwsn.cranemq.common.command.payloads.resp.MQSimplePullResponse;
import com.github.xjtuwsn.cranemq.common.command.types.AcquireResultType;
import com.github.xjtuwsn.cranemq.common.config.FlushDisk;
import com.github.xjtuwsn.cranemq.common.entity.Message;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.github.xjtuwsn.cranemq.common.entity.ReadyMessage;
import com.github.xjtuwsn.cranemq.common.route.QueueData;
import com.github.xjtuwsn.cranemq.common.utils.BrokerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @project:cranemq
 * @file:StoreService
 * @author:wsn
 * @create:2023/10/03-16:35
 */
public class MessageStoreCenter implements GeneralStoreService {
    private static final Logger log = LoggerFactory.getLogger(MessageStoreCenter.class);
    private BrokerController brokerController;
    private PersistentConfig persistentConfig;
    private CommitLog commitLog;
    private ConsumeQueueManager consumeQueueManager;
    private FlushDiskService flushDiskService;

    public MessageStoreCenter(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.persistentConfig = brokerController.getPersistentConfig();
        this.commitLog = new CommitLog(this.brokerController);
        this.consumeQueueManager = new ConsumeQueueManager(this.brokerController,
                this.brokerController.getPersistentConfig());
        if (persistentConfig.getFlushDisk() == FlushDisk.ASYNC) {
            this.flushDiskService = new AsyncFlushDiskService(persistentConfig, commitLog,
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
            if (persistentConfig.getFlushDisk() == FlushDisk.SYNC) {
                this.flushDiskService.flush(response.getMappedFile());
                this.flushDiskService.flush(putOffsetResp.getMappedFile());
            }
        }
        return putOffsetResp;

    }

    public QueueData createTopic(MQCreateTopicRequest mqCreateTopicRequest) {
        String topic = mqCreateTopicRequest.getTopic();
        int writeNumber = mqCreateTopicRequest.getWirteNumber();
        int readNumber = mqCreateTopicRequest.getReadNumber();
        return consumeQueueManager.createQueue(topic, writeNumber, readNumber);
    }

    public MQSimplePullResponse simplePullMessage(MQSimplePullRequest mqSimplePullRequest) {
        MessageQueue messageQueue = mqSimplePullRequest.getMessageQueue();

        int length = mqSimplePullRequest.getLength(), queueId = messageQueue.getQueueId();
        long offset = mqSimplePullRequest.getOffset();
        String topic = messageQueue.getTopic();
        List<Pair<Long, Integer>> commitLogData = new ArrayList<>();

        MQSimplePullResponse response = new MQSimplePullResponse(AcquireResultType.NO_MESSAGE);

        ConsumeQueue consumeQueue = consumeQueueManager.getConsumeQueue(topic, queueId);
        if (consumeQueue == null) {
            log.error("No such consume queue");
            return response;
        }

        MappedFile firstMappedFile = consumeQueue.getFirstMappedFile();
        if (firstMappedFile == null) {
            log.error("Consume queue has zero file");
            return response;
        }

        String firstName = firstMappedFile.getFileName();
        int queueUnit = persistentConfig.getQueueUnit();
        int maxQueueItemNumber = persistentConfig.getMaxQueueItemNumber();
        int left = length;
        //要么全都读到，要么全都不读
        for (int i = 0; i < length;) {
            int index = (int) ((offset + i) / maxQueueItemNumber);
            MappedFile current = consumeQueue.getMappedFileByIndex(index);
            if (current == null) {
                response.setResultType(AcquireResultType.OFFSET_INVALID);
                log.error("Offset has over the limit");
                return response;
            }
            int start = (int) ((offset + i) * queueUnit % maxQueueItemNumber);
            List<Pair<Long, Integer>> list = current.readOffsetIndex(start, left);
            if (left != 0 && list == null) {
                return response;
            }
            commitLogData.addAll(list);
            int readed = list.size();
            left -= readed;
            if (left == 0) {
                break;
            }
            i = length - left;
        }
        response.setResultType(AcquireResultType.ERROR);
        MappedFile firstCommit = commitLog.getFirstMappedFile();

        long nextOffset = offset + length;
        if (firstCommit == null) {
            return response;
        }
        String firstCommitName = firstCommit.getFileName();
        List<ReadyMessage> readyMessageList = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            Pair<Long, Integer> pair = commitLogData.get(i);

            long curOffset = pair.getKey();
            int curSize = pair.getValue();

            int mappedIndex = BrokerUtil.findMappedIndex(curOffset, firstCommitName,
                    persistentConfig.getCommitLogMaxSize());
            MappedFile mappedFileByIndex = commitLog.getMappedFileByIndex(mappedIndex);
            if (mappedFileByIndex == null) {
                return response;
            }
            int offsetInpage = BrokerUtil.offsetInPage(curOffset, persistentConfig.getCommitLogMaxSize());
            Message message = mappedFileByIndex.readSingleMessage(offsetInpage);
            if (message == null) {
                return response;
            }
            readyMessageList.add(new ReadyMessage(brokerController.getBrokerConfig().getBrokerName(),
                    queueId, offset + i, message));
        }
        response.setResultType(AcquireResultType.DONE);
        response.setNextOffset(nextOffset);
        response.setMessages(readyMessageList);
        return response;
    }
    @Override
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
