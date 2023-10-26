package com.github.xjtuwsn.cranemq.broker.store.queue;

import cn.hutool.core.util.StrUtil;
import com.github.xjtuwsn.cranemq.broker.BrokerController;
import com.github.xjtuwsn.cranemq.broker.store.CreateServiceThread;
import com.github.xjtuwsn.cranemq.broker.store.GeneralStoreService;
import com.github.xjtuwsn.cranemq.broker.store.MappedFile;
import com.github.xjtuwsn.cranemq.broker.store.PersistentConfig;
import com.github.xjtuwsn.cranemq.broker.store.cmtlog.DelayMessageCommitListener;
import com.github.xjtuwsn.cranemq.broker.store.cmtlog.RecoveryListener;
import com.github.xjtuwsn.cranemq.broker.store.comm.AsyncRequest;
import com.github.xjtuwsn.cranemq.broker.store.comm.PutMessageResponse;
import com.github.xjtuwsn.cranemq.broker.store.comm.StoreRequestType;
import com.github.xjtuwsn.cranemq.common.constant.MQConstant;
import com.github.xjtuwsn.cranemq.common.entity.QueueInfo;
import com.github.xjtuwsn.cranemq.common.route.QueueData;
import com.github.xjtuwsn.cranemq.common.utils.BrokerUtil;
import io.netty.util.internal.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @project:cranemq
 * @file:ConsumerQueueManager
 * @author:wsn
 * @create:2023/10/04-21:27
 * 消费队列的管理类
 */
public class ConsumeQueueManager implements GeneralStoreService {

    private static final Logger log = LoggerFactory.getLogger(ConsumeQueueManager.class);
    // 存放所有队列 topic : [id : queue]
    private ConcurrentHashMap<String, ConcurrentHashMap<Integer, ConsumeQueue>> queueTable = new ConcurrentHashMap<>();
    private BrokerController brokerController;
    private PersistentConfig persistentConfig;
    private CreateQueueService createQueueService;
    private RecoveryListener recoveryListener;

    private DelayMessageCommitListener delayMessageCommitListener;
    public ConsumeQueueManager(BrokerController brokerController, PersistentConfig persistentConfig) {
        this.brokerController = brokerController;
        this.persistentConfig = persistentConfig;
        this.createQueueService = new CreateQueueService();
    }
    @Override
    public void start() {
        File rootDir = new File(persistentConfig.getConsumerqueuePath());
        if (!rootDir.exists()) {
            rootDir.mkdir();
        }
        File[] topicFiles = rootDir.listFiles();
        for (File topicDir : topicFiles) {
            loadTopicQueueFile(topicDir);

        }
        // 创建默认队列和延时队列
        if (!queueTable.containsKey(MQConstant.DEFAULT_TOPIC_NAME)) {
            this.createQueue(MQConstant.DEFAULT_TOPIC_NAME, 4, 4);
        }
        if (!queueTable.containsKey(MQConstant.DELAY_TOPIC_NAME)) {
            this.createQueue(MQConstant.DELAY_TOPIC_NAME, 1, 1);
        }
        this.createQueueService.start();
        log.info("ConsumeQueue Manager start successfylly");
    }

    /**
     * 将commitlog信息写入队列
     * @param offset 原日志偏移
     * @param topic 主题
     * @param queueId
     * @param size
     * @param delay 延时时间
     * @return
     */
    public PutMessageResponse updateOffset(long offset, String topic, int queueId, int size, long delay) {
        // 如果不是延时消息，直接调用对应队列写入
        if (delay == 0) {
            ConsumeQueue queue = queueTable.get(topic).get(queueId);
            log.info("Select consume queue {}", queue);
            return queue.updateQueueOffset(offset, size);
        }
        // 延时消息，则将主题改为延时队列，写入到延时队列中，并且通知监听器延时消息存储完毕
        String newTopic = MQConstant.DELAY_TOPIC_NAME;
        ConsumeQueue queue = queueTable.get(newTopic).get(0);
        PutMessageResponse response = queue.updateQueueOffset(offset, size);
        long delayQueueOffset = response.getQueueOffset();
        this.delayMessageCommitListener.onCommit(offset, delayQueueOffset, topic, queueId, delay);
        return response;

    }

    /**
     * 从磁盘加载队列
     * @param topicDir
     */
    private void loadTopicQueueFile(File topicDir) {
        String topic = topicDir.getName();
        ConcurrentHashMap<Integer, ConsumeQueue> queueConcurrentHashMap = new ConcurrentHashMap<>();
        this.queueTable.put(topic, queueConcurrentHashMap);
        for (File queues : topicDir.listFiles()) {
            String queueIdStr = queues.getName();
            if (StrUtil.isNumeric(queueIdStr)) {
                int queueId = Integer.parseInt(queueIdStr);
                ConsumeQueue consumeQueue = new ConsumeQueue(queueId, topic, this.persistentConfig);
                queueConcurrentHashMap.put(queueId, consumeQueue);
                // 为每个队列注册监听器
                consumeQueue.registerCreateListener(new CreateRequestListener() {
                    @Override
                    public MappedFile onRequireCreate(String topic, int queueId, int index) {
                        return createQueueService.putCreateRequest(index, topic, queueId);
                    }
                });
                consumeQueue.registerUpdateOffsetListener(this.recoveryListener);
                consumeQueue.start();
            }
        }
    }
    public void registerRecoveryListener(RecoveryListener listener) {
        this.recoveryListener = listener;
    }
    public void registerDelayListener(DelayMessageCommitListener listener) {
        this.delayMessageCommitListener = listener;
    }
    public Iterator<ConcurrentHashMap<Integer, ConsumeQueue>> iterator() {
        Iterator<ConcurrentHashMap<Integer, ConsumeQueue>> iterator = queueTable.values().iterator();
        return iterator;
    }

    /**
     * 创建并加载主题和对应的队列
     * @param topic
     * @param writeNumber
     * @param readNumber
     * @return
     */
    public synchronized QueueData createQueue(String topic, int writeNumber, int readNumber) {
        QueueData res = new QueueData(brokerController.getBrokerConfig().getBrokerName());
        if (queueTable.containsKey(topic)) {
            int number = queueTable.get(topic).size();
            res.setWriteQueueNums(number);
            res.setReadQueueNums(number);
            return res;
        }
        String path = persistentConfig.getConsumerqueuePath() + topic + "\\";
        File rootDir = new File(path);
        rootDir.mkdir();
        for (int i = 0; i < writeNumber; i++) {
            String filePath = path + i + "\\";
            File queue = new File(filePath);
            queue.mkdir();
        }
        loadTopicQueueFile(rootDir);
        log.info("Finish create {} consume queue, write-number is {}", topic, writeNumber);
        return new QueueData(brokerController.getBrokerConfig().getBrokerName(), writeNumber, readNumber);
    }

    public boolean containsTopic(String topic) {
        return queueTable.containsKey(topic);
    }
    public ConsumeQueue getConsumeQueue(String topic, int queueId) {
        ConcurrentHashMap<Integer, ConsumeQueue> queueConcurrentHashMap = queueTable.get(topic);
        if (queueConcurrentHashMap == null) {
            return null;
        }
        return queueConcurrentHashMap.get(queueId);
    }
    public int getQueueNumber(String topic) {
        if (!queueTable.containsKey(topic)) {
            return -1;
        }
        return queueTable.get(topic).size();
    }
    public long getQueueCurWritePos(String topic, int queueId) {
        ConsumeQueue consumeQueue = queueTable.get(topic).get(queueId);
        if (consumeQueue == null) {
            return -1;
        }
        return consumeQueue.currentLastOffset();
    }
    public Map<String, QueueData> getAllQueueData() {
        Map<String, QueueData> map = new HashMap<>();
        for (Map.Entry<String, ConcurrentHashMap<Integer, ConsumeQueue>> entry : queueTable.entrySet()) {
            String topic = entry.getKey();
            int number = entry.getValue().size();

            QueueData queueData = new QueueData(this.brokerController.getBrokerConfig().getBrokerName(), number, number);
            map.put(topic, queueData);
        }
        return map;
    }

    public Map<String, List<QueueInfo>> allQueueInfos() {
        Map<String, List<QueueInfo>> map = new HashMap<>();
        for (Map.Entry<String, ConcurrentHashMap<Integer, ConsumeQueue>> entry : queueTable.entrySet()) {
            String topic = entry.getKey();
            List<QueueInfo> list = new ArrayList<>();
            ConcurrentHashMap<Integer, ConsumeQueue> queueMap = entry.getValue();
            for (Map.Entry<Integer, ConsumeQueue> inner : queueMap.entrySet()) {
                int queueId = inner.getKey();
                ConsumeQueue q = inner.getValue();
                long write = q.currentTotalWritePos();
                long flush = q.currentTotalFlushPos();
                long messages = q.currentTotalMessages();
                long lastModified = q.lastModified();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss");
                String formatStr = sdf.format(new Date(lastModified));
                list.add(new QueueInfo(topic, queueId, write, flush, messages, formatStr));
            }
            list.sort(Comparator.comparingInt(QueueInfo::getQueueId));
            map.put(topic, list);
        }
        return map;
    }
    @Override
    public void close() {
        for (Map<Integer, ConsumeQueue> consumeQueueMap : queueTable.values()) {
            for (ConsumeQueue queue : consumeQueueMap.values()) {
                queue.close();
            }
        }
    }

    /**
     * 创建队列mappedFile 的线程
     */
    class CreateQueueService extends CreateServiceThread {
        private final Logger log = LoggerFactory.getLogger(CreateQueueService.class);
        private ConcurrentHashMap<String, ConcurrentHashMap<Integer, AtomicLong>> lastCreateOffset =
                new ConcurrentHashMap<>();

        public CreateQueueService() {
            super();
        }

        @Override
        protected boolean createLoop() {
            try {
                AsyncRequest request = this.requestQueue.poll(3000, TimeUnit.MILLISECONDS);
                if (request == null) {
                    return true;
                }
                String key = request.getKey();
                AsyncRequest expect = this.requestTable.get(key);
                if (key == null) {
                    log.warn("Key {} has been removed before", key);
                    return true;
                }
                if (expect != request) {
                    log.error("Expected to be the same request");
                    return true;
                }
                String fullPath = BrokerUtil.getQueuePath(persistentConfig.getConsumerqueuePath(), request.getTopic(),
                        request.getQueueId(), request.getFileName());
                MappedFile mappedFile = new MappedFile(request.getIndex(), request.getFileSize(), request.getFileName(),
                        fullPath, persistentConfig);
                ConsumeQueue consumeQueue = queueTable.get(request.getTopic()).get(request.getQueueId());
                if (consumeQueue.appendMappedFile(mappedFile)) {
                    if (!lastCreateOffset.containsKey(request.getTopic())) {
                        lastCreateOffset.put(request.getTopic(), new ConcurrentHashMap<>());
                    }
                    ConcurrentHashMap<Integer, AtomicLong> map = lastCreateOffset.get(request.getTopic());
                    AtomicLong last = map.get(request.getQueueId());
                    long newVal = request.getIndex() * (long) persistentConfig.getMaxQueueSize();
                    if (last != null) {
                        last.getAndSet(newVal);
                    } else {
                        map.put(request.getQueueId(), new AtomicLong(newVal));
                    }
                    mappedFile.setWritePointer(0);
                    mappedFile.setCommitPointer(0);
                    mappedFile.setFlushPointer(0);
                    request.getCount().countDown();
                    log.info("Success create mapped file in consumer queue, topic: {}, queueId: {}, file: {}",
                            request.getTopic(), request.getQueueId(), request.getFileName());
                }
            } catch (InterruptedException e) {
                log.error("InterruptedException error in create loop");
                return false;
            }
            return true;
        }

        @Override
        public MappedFile putCreateRequest(int index, String topic, int queueId) {
            log.info("Create queue request, {}, {}, {}", topic, queueId, index);
            AtomicLong last = null;
            if (lastCreateOffset.containsKey(topic) && (last = lastCreateOffset.get(topic).get(queueId)) != null) {
                if (index * (long) persistentConfig.getMaxQueueSize() <= last.get()) {
                    log.info("{} has been created", index);
                    return queueTable.get(topic).get(queueId).getLastMappedFile();
                }
            }
            int fileSize = persistentConfig.getMaxQueueSize();
            String fileName = BrokerUtil.makeFileName(index, fileSize);
            AsyncRequest request = new AsyncRequest(index, fileName, fileSize, topic, queueId);
            StringBuilder key = new StringBuilder();
            key.append(topic).append("-").append(queueId).append("-").append(fileName);
            request.setKey(key.toString());
            this.requestTable.put(key.toString(), request);
            CountDownLatch count = new CountDownLatch(1);
            request.setCount(count);

            this.requestQueue.offer(request);

            try {
                count.await();
                ConsumeQueue consumeQueue = queueTable.get(topic).get(queueId);
                MappedFile lastMappedFile = consumeQueue.getLastMappedFile();
                if (lastMappedFile == null) {
                    log.error("Create failed");
                }
                this.requestTable.remove(key);
                return lastMappedFile;
            } catch (InterruptedException e) {
                log.error("InterruptedException error");
            }
            return null;
        }
        @Override
        protected MappedFile putCreateRequest(int index) {
            return null;
        }
    }
}
