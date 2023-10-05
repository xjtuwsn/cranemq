package com.github.xjtuwsn.cranemq.broker.store.queue;

import cn.hutool.core.util.StrUtil;
import com.github.xjtuwsn.cranemq.broker.BrokerController;
import com.github.xjtuwsn.cranemq.broker.store.CreateServiceThread;
import com.github.xjtuwsn.cranemq.broker.store.GeneralStoreService;
import com.github.xjtuwsn.cranemq.broker.store.MappedFile;
import com.github.xjtuwsn.cranemq.broker.store.PersistentConfig;
import com.github.xjtuwsn.cranemq.broker.store.cmtlog.RecoveryListener;
import com.github.xjtuwsn.cranemq.broker.store.comm.AsyncRequest;
import com.github.xjtuwsn.cranemq.broker.store.comm.PutMessageResponse;
import com.github.xjtuwsn.cranemq.broker.store.comm.StoreRequestType;
import com.github.xjtuwsn.cranemq.common.utils.BrokerUtil;
import io.netty.util.internal.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @project:cranemq
 * @file:ConsumerQueueManager
 * @author:wsn
 * @create:2023/10/04-21:27
 */
public class ConsumeQueueManager implements GeneralStoreService {

    private static final Logger log = LoggerFactory.getLogger(ConsumeQueueManager.class);
    private ConcurrentHashMap<String, ConcurrentHashMap<Integer, ConsumeQueue>> queueTable = new ConcurrentHashMap<>();
    private BrokerController brokerController;
    private PersistentConfig persistentConfig;
    private CreateQueueService createQueueService;
    private RecoveryListener recoveryListener;
    public ConsumeQueueManager(BrokerController brokerController, PersistentConfig persistentConfig) {
        this.brokerController = brokerController;
        this.persistentConfig = persistentConfig;
        this.createQueueService = new CreateQueueService();
    }
    public PutMessageResponse updateOffset(long offset, String topic, int queueId, int size) {
        ConsumeQueue queue = queueTable.get(topic).get(queueId);
        log.info("Select consume queue {}", queue);
        return queue.updateQueueOffset(offset, size);
    }

    public void start() {
        File rootDir = new File(persistentConfig.getConsumerqueuePath());
        if (!rootDir.exists()) {
            rootDir.mkdir();
        }
        File[] topicFiles = rootDir.listFiles();
        for (File topicDir : topicFiles) {
            String topic = topicDir.getName();
            ConcurrentHashMap<Integer, ConsumeQueue> queueConcurrentHashMap = new ConcurrentHashMap<>();
            this.queueTable.put(topic, queueConcurrentHashMap);
            for (File queues : topicDir.listFiles()) {
                String queueIdStr = queues.getName();
                if (StrUtil.isNumeric(queueIdStr)) {
                    int queueId = Integer.parseInt(queueIdStr);
                    ConsumeQueue consumeQueue = new ConsumeQueue(queueId, topic, this.persistentConfig);
                    queueConcurrentHashMap.put(queueId, consumeQueue);
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
        this.createQueueService.start();
        log.info("ConsumeQueue Manager start successfylly");
    }

    public void registerRecoveryListener(RecoveryListener listener) {
        this.recoveryListener = listener;
    }

    @Override
    public void close() {

    }
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
