package com.github.xjtuwsn.cranemq.broker.store;

import cn.hutool.core.lang.Pair;
import com.github.xjtuwsn.cranemq.broker.BrokerController;
import com.github.xjtuwsn.cranemq.broker.store.cmtlog.CommitEntry;
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
import com.github.xjtuwsn.cranemq.common.command.types.RequestType;
import com.github.xjtuwsn.cranemq.common.config.FlushDisk;
import com.github.xjtuwsn.cranemq.common.entity.Message;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.github.xjtuwsn.cranemq.common.entity.ReadyMessage;
import com.github.xjtuwsn.cranemq.common.route.QueueData;
import com.github.xjtuwsn.cranemq.common.utils.BrokerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

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
    private TransmitCommitLogService transmitCommitLogService;

    public MessageStoreCenter(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.persistentConfig = brokerController.getPersistentConfig();
        this.commitLog = new CommitLog(this.brokerController, this);
        this.consumeQueueManager = new ConsumeQueueManager(this.brokerController,
                this.brokerController.getPersistentConfig());
        if (persistentConfig.getFlushDisk() == FlushDisk.ASYNC) {
            this.flushDiskService = new AsyncFlushDiskService(persistentConfig, commitLog,
                    consumeQueueManager);
        } else {
            this.flushDiskService = new SyncFlushDiskService();
        }
        if (persistentConfig.isEnableOutOfMemory()) {
            this.transmitCommitLogService = new TransmitCommitLogService();
        }
    }
    public PutMessageResponse putMessage(List<StoreInnerMessage> innerMessages) {
        if (innerMessages == null || innerMessages.isEmpty()) {
            return new PutMessageResponse(StoreResponseType.PARAMETER_ERROR);
        }
        PutMessageResponse response = new PutMessageResponse();
        for (StoreInnerMessage innerMessage : innerMessages) {
            PutMessageResponse res = this.putMessage(innerMessage);
            if (res.getResponseType() != StoreResponseType.STORE_OK) {
                log.error("Store batch message error");
            }
            response.setSize(response.getSize() + res.getSize());
            response.setOffset(res.getOffset());
        }
        response.setResponseType(StoreResponseType.STORE_OK);
        return response;
    }
    public PutMessageResponse putMessage(StoreInnerMessage innerMessage) {
        if (innerMessage == null) {
            log.error("Null put message request");
            return new PutMessageResponse(StoreResponseType.PARAMETER_ERROR);
        }
        long start = System.nanoTime();
        PutMessageResponse response = this.commitLog.writeMessage(innerMessage);
        long end1 = System.nanoTime();

        if (response.getResponseType() == StoreResponseType.PARAMETER_ERROR) {
            log.error("Put message to CommitLog error");

        }
        // 同步刷盘
        if (persistentConfig.getFlushDisk() == FlushDisk.SYNC) {
            this.flushDiskService.flush(response.getMappedFile());
        }
        // 没有提交这一步骤，每次put完都要同步刷到consumequeue
        if (!response.getMappedFile().ownDirectMemory()) {
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
            this.brokerController.getHoldRequestService().awakeNow(
                    Arrays.asList(new Pair<>(innerMessage.getTopic(), innerMessage.getQueueId())));
            // 同步刷盘
            if (putOffsetResp.getResponseType() == StoreResponseType.STORE_OK) {
                if (persistentConfig.getFlushDisk() == FlushDisk.SYNC) {
                    this.flushDiskService.flush(putOffsetResp.getMappedFile());
                }
            }
            /**
             * 唤醒push请求
             */
            return putOffsetResp;
        }


        return response;
    }

    private void putOffsetToQueue(List<CommitEntry> entries) {

        List<Pair<String, Integer>> queue = new ArrayList<>();

        for (CommitEntry entry : entries) {
            String topic = entry.getTopic(), tag = entry.getTag();
            long offset = entry.getOffset();
            int size = entry.getSize(), queueId = entry.getQueueId();
            queue.add(new Pair<>(topic, queueId));
            PutMessageResponse response = this.consumeQueueManager.updateOffset(offset, topic, queueId, size);

            // 同步刷盘
            if (persistentConfig.getFlushDisk() == FlushDisk.SYNC) {
                flushDiskService.flush(response.getMappedFile());
            }

        }
        this.brokerController.getHoldRequestService().awakeNow(queue);
        /**
         * 唤醒push请求
         */
    }

    public QueueData createTopic(MQCreateTopicRequest mqCreateTopicRequest) {
        String topic = mqCreateTopicRequest.getTopic();
        int writeNumber = mqCreateTopicRequest.getWirteNumber();
        int readNumber = mqCreateTopicRequest.getReadNumber();
        return consumeQueueManager.createQueue(topic, writeNumber, readNumber);
    }

    public int getQueueNumber(String topic) {
        int queueNumber = consumeQueueManager.getQueueNumber(topic);
        if (queueNumber == -1) {
            consumeQueueManager.createQueue(topic, persistentConfig.getDefaultQueueNumber(),
                    persistentConfig.getDefaultQueueNumber());
            queueNumber = persistentConfig.getDefaultQueueNumber();

        }
        return queueNumber;
    }

    public long getQueueCurWritePos(String topic, int queueId) {
        return consumeQueueManager.getQueueCurWritePos(topic, queueId);
    }

    public Pair<Pair<List<ReadyMessage>, Long>, AcquireResultType> read(String topic, int queueId, long offset, int length) {
        List<Pair<Long, Integer>> commitLogData = new ArrayList<>();

        AcquireResultType result = AcquireResultType.NO_MESSAGE;

        ConsumeQueue consumeQueue = consumeQueueManager.getConsumeQueue(topic, queueId);
        if (consumeQueue == null) {
            log.error("No such consume queue");
            return new Pair<>(null, result);
        }

        MappedFile firstMappedFile = consumeQueue.getFirstMappedFile();
        if (firstMappedFile == null) {
//            log.error("Consume queue has zero file, {}, {}", topic, queueId);
            return new Pair<>(null, result);
        }

        String firstName = firstMappedFile.getFileName();
        int queueUnit = persistentConfig.getQueueUnit();
        int maxQueueItemNumber = persistentConfig.getMaxQueueItemNumber();
        int left = length;
        for (int i = 0; i < length;) {
            int index = (int) ((offset + i) / maxQueueItemNumber);
            MappedFile current = consumeQueue.getMappedFileByIndex(index);
            if (current == null) {
//                 log.error("Offset has over the limit");
                break;
            }
            int start = (int) ((offset + i) % maxQueueItemNumber) * queueUnit;
            List<Pair<Long, Integer>> list = current.readOffsetIndex(start, left);
            if (list == null || list.size() == 0) {
//                log.warn("Read zero offset index, prove no more index");
                break;
            }
            commitLogData.addAll(list);
            int readed = list.size();
            left -= readed;
            if (left == 0) {
                break;
            }
            i = length - left;
        }

        if (commitLogData.size() == 0) {
            return new Pair<>(null, result);
        }
        result = AcquireResultType.ERROR;
        MappedFile firstCommit = commitLog.getFirstMappedFile();

        long nextOffset = offset + commitLogData.size();
        if (firstCommit == null) {
//             log.warn("There is no commitLog mapped file");
            return new Pair<>(null, result);
        }
        String firstCommitName = firstCommit.getFileName();
        List<ReadyMessage> readyMessageList = new ArrayList<>();
        for (int i = 0; i < Math.min(length, commitLogData.size()); i++) {
            Pair<Long, Integer> pair = commitLogData.get(i);

            long curOffset = pair.getKey();
            int curSize = pair.getValue();

            int mappedIndex = BrokerUtil.findMappedIndex(curOffset, firstCommitName,
                    persistentConfig.getCommitLogMaxSize());
            MappedFile mappedFileByIndex = commitLog.getMappedFileByIndex(mappedIndex);
            if (mappedFileByIndex == null) {
                System.out.println(offset);
                 log.warn("Doesnot have this message, problely something wrong, " +
                         "topic {}, queueId {}", topic, queueId);
                break;
            }
            int offsetInpage = BrokerUtil.offsetInPage(curOffset, persistentConfig.getCommitLogMaxSize());
            Message message = mappedFileByIndex.readSingleMessage(offsetInpage);
            if (message == null) {
//                 log.warn("Read null from mappedfile, offset record error");
                break;
            }
            readyMessageList.add(new ReadyMessage(brokerController.getBrokerConfig().getBrokerName(),
                    queueId, offset + i, message));
        }

        result = AcquireResultType.DONE;
        return new Pair<>(new Pair<>(readyMessageList, nextOffset), result);

    }

    public MQSimplePullResponse simplePullMessage(MQSimplePullRequest mqSimplePullRequest) {
        MessageQueue messageQueue = mqSimplePullRequest.getMessageQueue();

        int length = mqSimplePullRequest.getLength(), queueId = messageQueue.getQueueId();
        long offset = mqSimplePullRequest.getOffset();
        String topic = messageQueue.getTopic();

        Pair<Pair<List<ReadyMessage>, Long>, AcquireResultType> result = read(topic, queueId, offset, length);

        MQSimplePullResponse response = new MQSimplePullResponse();
        List<ReadyMessage> readyMessageList = result.getKey().getKey();

        long nextOffset = result.getKey().getValue();
        if (readyMessageList != null) {
            nextOffset += readyMessageList.size();
        }

        response.setResultType(AcquireResultType.DONE);
        response.setNextOffset(nextOffset);
        response.setMessages(readyMessageList);
        return response;
    }

    public Map<String, QueueData> getAllQueueData() {
        return this.consumeQueueManager.getAllQueueData();
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
        if (this.transmitCommitLogService != null) {
            this.transmitCommitLogService.start();
        }
    }

    @Override
    public void close() {
        this.commitLog.close();
        this.consumeQueueManager.close();
    }

    public TransmitCommitLogService getTransmitCommitLogService() {
        return transmitCommitLogService;
    }

    public void putEntries(List<CommitEntry> entries) {
        this.transmitCommitLogService.putEntries(entries);
    }

    class TransmitCommitLogService extends Thread {
        private final Logger log = LoggerFactory.getLogger(TransmitCommitLogService.class);

        private LinkedBlockingQueue<List<CommitEntry>> queue = new LinkedBlockingQueue<>(3000);

        private boolean isStop = false;
        @Override
        public void run() {
            while (!isStop) {
                try {
                    List<CommitEntry> entries = queue.take();
                    putOffsetToQueue(entries);
                } catch (InterruptedException e) {
                    log.error("TransmitCommitLogService has been Interrupted");
                }
            }
        }

        public void putEntries(List<CommitEntry> entries) {
            if (entries != null) {
                try {
                    queue.put(entries);
                } catch (InterruptedException e) {
                    log.error("TransmitCommitLogService has been Interrupted");
                }
            }
        }

        public void setStop(boolean stop) {
            isStop = stop;
        }
    }
}
