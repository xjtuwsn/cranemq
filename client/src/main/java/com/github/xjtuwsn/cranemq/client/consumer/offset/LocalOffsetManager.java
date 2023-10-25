package com.github.xjtuwsn.cranemq.client.consumer.offset;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.github.xjtuwsn.cranemq.client.remote.ClientInstance;
import com.github.xjtuwsn.cranemq.common.constant.MQConstant;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.github.xjtuwsn.cranemq.common.utils.BrokerUtil;
import com.github.xjtuwsn.cranemq.common.utils.JSONUtil;
import com.github.xjtuwsn.cranemq.common.utils.TopicUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @project:cranemq
 * @file:LocalOffsetManager
 * @author:wsn
 * @create:2023/10/11-19:35
 * 本地消费位移管理
 */
public class LocalOffsetManager implements OffsetManager {

    private static final Logger log = LoggerFactory.getLogger(LocalOffsetManager.class);
    private String storeName;
    private ConcurrentHashMap<String, ConcurrentHashMap<MessageQueue, Long>> offsetTable = new ConcurrentHashMap<>();
    private ClientInstance clientInstance;

    private ScheduledExecutorService persistOffsetTimer;
    private AtomicBoolean started = new AtomicBoolean(false);
    private String fileName;
    private String path = MQConstant.DEFAULT_LOCAL_OFFSET_PATH;
    private String suffix = MQConstant.LOCAL_OFFSET_SUFFIX;
    public LocalOffsetManager(ClientInstance clientInstance) {
        this.clientInstance = clientInstance;
        this.persistOffsetTimer = new ScheduledThreadPoolExecutor(1);
    }
    @Override
    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        this.storeName = TopicUtil.buildStoreID(clientInstance.getClientId());
        File root = new File(path);
        if (!root.exists()) {
            root.mkdir();
        }
        this.fileName = path + storeName + suffix;
        String s = JSONUtil.fileToString(fileName);
        offsetTable = JSONObject.parseObject(s,
                new TypeReference<ConcurrentHashMap<String, ConcurrentHashMap<MessageQueue, Long>>>(){});
        if (offsetTable == null) {
            offsetTable = new ConcurrentHashMap<>();
        }
        this.persistOffsetTimer.scheduleAtFixedRate(() -> {
            persistOffset();
        }, 1000, 5 * 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized void record(MessageQueue messageQueue, long offset, String group) {
        if (messageQueue == null || offset < 0 || StrUtil.isEmpty(group)) {
            return;
        }
        ConcurrentHashMap<MessageQueue, Long> queueOffset = offsetTable.get(group);
        if (queueOffset == null) {
            queueOffset = new ConcurrentHashMap<>();
            offsetTable.put(group, queueOffset);
        }
        queueOffset.put(messageQueue, offset);
    }

    @Override
    public long readOffset(MessageQueue messageQueue, String group) {
        if (messageQueue == null || StrUtil.isEmpty(group)) {
            return -1;
        }
        ConcurrentHashMap<MessageQueue, Long> map = offsetTable.get(group);
        if (map == null || !map.containsKey(messageQueue)) {
            return -1;
        }
        return map.get(messageQueue);
    }

    @Override
    public void resetLocalOffset(String group, Map<MessageQueue, Long> allOffsets) {

    }

    @Override
    public void persistOffset() {
        if (offsetTable == null || offsetTable.isEmpty()) {
            return;
        }
        JSONUtil.JSONStrToFile(offsetTable, fileName);
    }
}
