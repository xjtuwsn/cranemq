package com.github.xjtuwsn.cranemq.broker.store;

import cn.hutool.core.lang.Pair;
import com.github.xjtuwsn.cranemq.broker.BrokerController;
import com.github.xjtuwsn.cranemq.broker.store.cmtlog.CommitEntry;
import com.github.xjtuwsn.cranemq.broker.store.cmtlog.CommitLog;
import com.github.xjtuwsn.cranemq.broker.store.cmtlog.DelayMessageCommitListener;
import com.github.xjtuwsn.cranemq.broker.store.cmtlog.RecoveryListener;
import com.github.xjtuwsn.cranemq.broker.store.comm.PutMessageResponse;
import com.github.xjtuwsn.cranemq.broker.store.comm.StoreInnerMessage;
import com.github.xjtuwsn.cranemq.broker.store.comm.StoreResponseType;
import com.github.xjtuwsn.cranemq.broker.store.flush.AsyncFlushDiskService;
import com.github.xjtuwsn.cranemq.broker.store.flush.FlushDiskService;
import com.github.xjtuwsn.cranemq.broker.store.flush.SyncFlushDiskService;
import com.github.xjtuwsn.cranemq.broker.store.queue.ConsumeQueue;
import com.github.xjtuwsn.cranemq.broker.store.queue.ConsumeQueueManager;
import com.github.xjtuwsn.cranemq.broker.timer.DelayMessageTask;
import com.github.xjtuwsn.cranemq.broker.timer.DelayTask;
import com.github.xjtuwsn.cranemq.broker.timer.TimingWheel;
import com.github.xjtuwsn.cranemq.common.command.payloads.req.MQCreateTopicRequest;
import com.github.xjtuwsn.cranemq.common.command.payloads.req.MQSimplePullRequest;
import com.github.xjtuwsn.cranemq.common.command.payloads.resp.MQSimplePullResponse;
import com.github.xjtuwsn.cranemq.common.command.types.AcquireResultType;
import com.github.xjtuwsn.cranemq.common.command.types.RequestType;
import com.github.xjtuwsn.cranemq.common.config.FlushDisk;
import com.github.xjtuwsn.cranemq.common.constant.MQConstant;
import com.github.xjtuwsn.cranemq.common.entity.Message;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.github.xjtuwsn.cranemq.common.entity.QueueInfo;
import com.github.xjtuwsn.cranemq.common.entity.ReadyMessage;
import com.github.xjtuwsn.cranemq.common.route.QueueData;
import com.github.xjtuwsn.cranemq.common.utils.BrokerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @project:cranemq
 * @file:StoreService
 * @author:wsn
 * @create:2023/10/03-16:35
 */

/**
 * 消息存储管理中心
 * @author wsn
 */
public class MessageStoreCenter implements GeneralStoreService {
    private static final Logger log = LoggerFactory.getLogger(MessageStoreCenter.class);
    private BrokerController brokerController;
    private PersistentConfig persistentConfig;
    // commitLog管理类
    private CommitLog commitLog;
    // 消费队列管理
    private ConsumeQueueManager consumeQueueManager;
    // 刷盘服务
    private FlushDiskService flushDiskService;
    // 将提交的commitLog项进行转送到索引文件记录
    private TransmitCommitLogService transmitCommitLogService;

    // 延时消息日志管理
    private TimingWheelLog timingWheelLog;

    // 时间轮
    private TimingWheel<DelayTask> timingWheel;

    public MessageStoreCenter(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.persistentConfig = brokerController.getPersistentConfig();
        this.commitLog = new CommitLog(this.brokerController, this);
        this.consumeQueueManager = new ConsumeQueueManager(this.brokerController,
                this.brokerController.getPersistentConfig());
        // 根据刷盘策略不同初始化刷盘服务
        if (persistentConfig.getFlushDisk() == FlushDisk.ASYNC) {
            this.flushDiskService = new AsyncFlushDiskService(persistentConfig, commitLog,
                    consumeQueueManager);
        } else {
            this.flushDiskService = new SyncFlushDiskService();
        }
        if (persistentConfig.isEnableOutOfMemory()) {
            this.transmitCommitLogService = new TransmitCommitLogService();
        }
        this.timingWheel = new TimingWheel<>();
        this.timingWheelLog = new TimingWheelLog(this.brokerController);
    }

    /**
     * 批量写入消息
     * @param innerMessages 待写入消息的内部封装列表
     * @return
     */
    public PutMessageResponse putMessage(List<StoreInnerMessage> innerMessages) {
        if (innerMessages == null || innerMessages.isEmpty()) {
            return new PutMessageResponse(StoreResponseType.PARAMETER_ERROR);
        }
        PutMessageResponse response = new PutMessageResponse();
        // 分别调用putMessage，更新偏移
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

    /**
     * 单个消息的写入，写入commitLog
     * @param innerMessage
     * @return
     */
    public PutMessageResponse putMessage(StoreInnerMessage innerMessage) {
        if (innerMessage == null) {
            log.error("Null put message request");
            return new PutMessageResponse(StoreResponseType.PARAMETER_ERROR);
        }
        long start = System.nanoTime();
        // 调用commitlog，写入消息
        PutMessageResponse response = this.commitLog.writeMessage(innerMessage);
        long end1 = System.nanoTime();

        if (response.getResponseType() == StoreResponseType.PARAMETER_ERROR) {
            log.error("Put message to CommitLog error");

        }
        // 同步刷盘，立即刷盘
        if (persistentConfig.getFlushDisk() == FlushDisk.SYNC) {
            this.flushDiskService.flush(response.getMappedFile());
        }
        // 没有提交这一步骤，每次put完都要同步刷到consumequeue
        if (!response.getMappedFile().ownDirectMemory()) {
            PutMessageResponse putOffsetResp = null;
            if (response.getResponseType() == StoreResponseType.STORE_OK) {
                long offset = response.getOffset();
                int size = response.getSize();
                // 将刚写入的信息更新到队列索引中
                putOffsetResp = this.consumeQueueManager.updateOffset(offset, innerMessage.getTopic(),
                        innerMessage.getQueueId(), size, innerMessage.getDelay());
            }
            if (putOffsetResp == null) {
                return new PutMessageResponse(StoreResponseType.PARAMETER_ERROR);
            }
            // 如果这个消息不是延迟消息，需要唤醒监听这个主题的长轮询连接
            if (innerMessage.getDelay() == 0) {
                this.brokerController.getHoldRequestService().awakeNow(
                        Arrays.asList(new Pair<>(innerMessage.getTopic(), innerMessage.getQueueId())));
            }
            // 同步刷盘
            if (putOffsetResp.getResponseType() == StoreResponseType.STORE_OK) {
                if (persistentConfig.getFlushDisk() == FlushDisk.SYNC) {
                    this.flushDiskService.flush(putOffsetResp.getMappedFile());
                }
            }

            return putOffsetResp;
        }


        return response;
    }

    /**
     * 将CommitEntry，已经提交的信息转写到消费队列中
     * @param entries
     */
    private void putOffsetToQueue(List<CommitEntry> entries) {

        List<Pair<String, Integer>> queue = new ArrayList<>();

        for (CommitEntry entry : entries) {
            String topic = entry.getTopic(), tag = entry.getTag();
            long offset = entry.getOffset(), delay = entry.getDelay();
            int size = entry.getSize(), queueId = entry.getQueueId();
            if (delay == 0) {
                queue.add(new Pair<>(topic, queueId));
            }
            // 更新消费队列
            PutMessageResponse response = this.consumeQueueManager.updateOffset(offset, topic, queueId, size, delay);

            // 同步刷盘
            if (persistentConfig.getFlushDisk() == FlushDisk.SYNC) {
                flushDiskService.flush(response.getMappedFile());
            }

        }
        // 唤醒push请求
        this.brokerController.getHoldRequestService().awakeNow(queue);
    }

    /**
     * 创建新的topic
     * @param mqCreateTopicRequest
     * @return
     */
    public QueueData createTopic(MQCreateTopicRequest mqCreateTopicRequest) {
        String topic = mqCreateTopicRequest.getTopic();
        int writeNumber = mqCreateTopicRequest.getWirteNumber();
        int readNumber = mqCreateTopicRequest.getReadNumber();
        if (topic.startsWith(MQConstant.RETRY_PREFIX)) {
            writeNumber = 1;
            readNumber =1;
        }
        return consumeQueueManager.createQueue(topic, writeNumber, readNumber);
    }

    /**
     * retry队列和死信队列单独创建
     * @param topic
     */
    public void checkDlqAndRetry(String topic) {
        if (!consumeQueueManager.containsTopic(topic)) {
            consumeQueueManager.createQueue(topic, 1, 1);
            this.brokerController.updateRegistry();
        }
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

    /**
     * 从commitLog中读指定的消息
     * @param topic 主题
     * @param queueId 队列id
     * @param offset 起始队列偏移
     * @param length 读取消息数
     * @return 返回消息列表和下一次读的偏移
     */
    public Pair<Pair<List<ReadyMessage>, Long>, AcquireResultType> read(String topic, int queueId, long offset, int length) {
        // 从队列中读到的，在commitLog中的偏移
        List<Pair<Long, Integer>> commitLogData = new ArrayList<>();

        AcquireResultType result = AcquireResultType.NO_MESSAGE;

        // 获取到主题和id对应的消费队列
        ConsumeQueue consumeQueue = consumeQueueManager.getConsumeQueue(topic, queueId);
        if (consumeQueue == null) {
            log.error("No such consume queue");
            return new Pair<>(null, result);
        }

        // 获得消费队列的第一个mappedfile文件，用于根据偏移计算索引
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
            // 根据偏移和头文件名计算当前该哪一个文件
            int index = (int) ((offset + i) / maxQueueItemNumber);
            MappedFile current = consumeQueue.getMappedFileByIndex(index);
            if (current == null) {
                break;
            }
            // 计算在文件内的偏移
            int start = (int) ((offset + i) % maxQueueItemNumber) * queueUnit;
            // 批量读取索引信息
            List<Pair<Long, Integer>> list = current.readOffsetIndex(start, left);
            if (list == null || list.size() == 0) {
                break;
            }
            commitLogData.addAll(list);
            int readed = list.size();
            // 更新剩余数量
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
        // 相同的步骤，计算commitLog的索引信息
        MappedFile firstCommit = commitLog.getFirstMappedFile();

        long nextOffset = offset + commitLogData.size();
        if (firstCommit == null) {
            return new Pair<>(null, result);
        }
        String firstCommitName = firstCommit.getFileName();
        List<ReadyMessage> readyMessageList = new ArrayList<>();
        for (int i = 0; i < Math.min(length, commitLogData.size()); i++) {
            Pair<Long, Integer> pair = commitLogData.get(i);

            long curOffset = pair.getKey();
            int curSize = pair.getValue();

            // 根据索引找到偏移对应的mappedFile
            int mappedIndex = BrokerUtil.findMappedIndex(curOffset, firstCommitName,
                    persistentConfig.getCommitLogMaxSize());
            MappedFile mappedFileByIndex = commitLog.getMappedFileByIndex(mappedIndex);
            if (mappedFileByIndex == null) {
                 log.warn("Doesnot have this message, problely something wrong, " +
                         "topic {}, queueId {}", topic, queueId);
                break;
            }
            // 计算总的偏移在当前页内的偏移
            int offsetInpage = BrokerUtil.offsetInPage(curOffset, persistentConfig.getCommitLogMaxSize());
            // 读取偏移处对应的消息
            StoreInnerMessage innerMessage = mappedFileByIndex.readSingleMessage(offsetInpage);
            if (innerMessage == null) {
                break;
            }
            Message message = innerMessage.getMessage();
            readyMessageList.add(new ReadyMessage(brokerController.getBrokerConfig().getBrokerName(),
                    queueId, offset + i, message, innerMessage.getRetry()));
        }

        result = AcquireResultType.DONE;
        return new Pair<>(new Pair<>(readyMessageList, nextOffset), result);

    }

    /**
     * 根据commitLog偏移读取单条消息
     * @param offset
     * @return
     */
    public StoreInnerMessage readSingleMessage(long offset) {
        MappedFile firstCommit = commitLog.getFirstMappedFile();
        String firstCommitName = firstCommit.getFileName();
        int mappedIndex = BrokerUtil.findMappedIndex(offset, firstCommitName,
                persistentConfig.getCommitLogMaxSize());
        MappedFile mappedFileByIndex = commitLog.getMappedFileByIndex(mappedIndex);
        if (mappedFileByIndex == null) {
            log.warn("Doesnot have this message, problely something wrong");
            return null;
        }
        int offsetInpage = BrokerUtil.offsetInPage(offset, persistentConfig.getCommitLogMaxSize());
        StoreInnerMessage message = mappedFileByIndex.readSingleMessage(offsetInpage);
        return message;
    }

    /**
     * 消费者主动拉取消息
     * @param mqSimplePullRequest
     * @return
     */
    public MQSimplePullResponse simplePullMessage(MQSimplePullRequest mqSimplePullRequest) {
        MessageQueue messageQueue = mqSimplePullRequest.getMessageQueue();

        // 拉取的消息信息，数量、偏移等
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

    /**
     * 延时消息是先写入commitLog，直到提交时才将其写入延时队列，并且开始延时计时
     * @param commitLogOffset 在commitLog中的偏移
     * @param queueOffset 延时队列中的偏移
     * @param topic  延时之后将要投放到的队列
     * @param queueId 对应队列id
     * @param delay 延迟时间
     * @param id 写入到延迟日志中的事务id
     */
    public void onCommitDelayMessage(long commitLogOffset, long queueOffset, String topic, int queueId, long delay, String id) {
        if (id == null) {
            log.error("Time wheel appendlog error");
            return;
        }
        // 向延时器中提交任务
        timingWheel.submit(new DelayMessageTask(brokerController, topic, commitLogOffset, queueOffset, queueId, id),
                delay, TimeUnit.SECONDS);
    }
    public void onCommitDelayMessage(long commitLogOffset, long queueOffset, String topic, int queueId, long delay) {
        // 向延时日志写入延时任务
        String id = timingWheelLog.appendLog(topic, commitLogOffset, queueOffset, queueId, delay);
        this.onCommitDelayMessage(commitLogOffset, queueOffset, topic, queueId, delay, id);
    }

    public Map<String, QueueData> getAllQueueData() {
        return this.consumeQueueManager.getAllQueueData();
    }

    public Map<String, List<QueueInfo>> getAllQueueInfos() {
        return this.consumeQueueManager.allQueueInfos();
    }
    @Override
    public void start() {
        // 向消费队列管理器中注册日志恢复监听器，用于启动时从消费队列中找到最大写入位移，然后将commitLog写指针重置
        this.consumeQueueManager.registerRecoveryListener(new RecoveryListener() {
            @Override
            public void onUpdateOffset(long offset, int size) {
                commitLog.recoveryFromQueue(offset, size);
            }
        });
        // 注册延时消息提交的监听器，当延时消息写道延时队列之后触发
        this.consumeQueueManager.registerDelayListener(new DelayMessageCommitListener() {
            @Override
            public void onCommit(long commitLogOffset, long queueOffset, String topic, int queueId, long delay) {
                onCommitDelayMessage(commitLogOffset, queueOffset, topic, queueId, delay);
            }
        });
        this.createDir();
        this.consumeQueueManager.start();
        this.commitLog.start();
        if (this.flushDiskService instanceof AsyncFlushDiskService) {
            ((AsyncFlushDiskService) flushDiskService).start();
            log.info("Async flush disk service start successfully");
        }
        if (this.transmitCommitLogService != null) {
            this.transmitCommitLogService.start();
        }
        this.timingWheelLog.start();
    }

    @Override
    public void close() {
        this.commitLog.close();
        this.consumeQueueManager.close();
        this.timingWheelLog.close();
    }

    private void createDir() {
        try {
            File craneFile = new File(persistentConfig.getCranePath());
            if (!craneFile.exists()) {
                craneFile.mkdir();
            }
            File rootFile = new File(persistentConfig.getRootPath());
            if (!rootFile.exists()) {
                rootFile.mkdir();
            }
            File cmtlogFile = new File(persistentConfig.getCommitLogPath());
            if (!cmtlogFile.exists()) {
                cmtlogFile.mkdir();
            }
            File queueFile = new File(persistentConfig.getConsumerqueuePath());
            if (!queueFile.exists()) {
                queueFile.mkdir();
            }
            File delayFile = new File(persistentConfig.getDelayLogPath());
            if (!delayFile.exists()) {
                delayFile.mkdir();
            }
            File configFile = new File(persistentConfig.getConfigPath());
            if (!configFile.exists()) {
                configFile.mkdir();
            }
        } catch (Exception e) {
            log.error("Create dir file error");
        }
    }

    public TimingWheelLog getTimingWheelLog() {
        return timingWheelLog;
    }

    public TransmitCommitLogService getTransmitCommitLogService() {
        return transmitCommitLogService;
    }

    public void putEntries(List<CommitEntry> entries) {
        this.transmitCommitLogService.putEntries(entries);
    }

    /**
     * 负责将已提交的消息转送到对应队列
     */
    class TransmitCommitLogService extends Thread {
        private final Logger log = LoggerFactory.getLogger(TransmitCommitLogService.class);

        private LinkedBlockingQueue<List<CommitEntry>> queue = new LinkedBlockingQueue<>(3000);

        private boolean isStop = false;
        @Override
        public void run() {
            while (!isStop) {
                try {
                    // 从阻塞队列中拿到提交的entry，写入
                    List<CommitEntry> entries = queue.take();
                    putOffsetToQueue(entries);
                } catch (InterruptedException e) {
                    log.error("TransmitCommitLogService has been Interrupted");
                }
            }
        }

        /**
         * 向阻塞队列中放入提交信息
         * @param entries
         */
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
