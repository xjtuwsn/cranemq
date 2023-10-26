package com.github.xjtuwsn.cranemq.client.consumer.offset;

import com.github.xjtuwsn.cranemq.client.remote.ClientInstance;
import com.github.xjtuwsn.cranemq.common.command.Header;
import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.command.payloads.req.MQRecordOffsetRequest;
import com.github.xjtuwsn.cranemq.common.command.types.RequestType;
import com.github.xjtuwsn.cranemq.common.command.types.RpcType;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.github.xjtuwsn.cranemq.common.utils.TopicUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * @project:cranemq
 * @file:BrokerOffsetManager
 * @author:wsn
 * @create:2023/10/10-15:12
 * 让broker进行消费者组位移管理，本地向服务器提交位移
 */
public class BrokerOffsetManager implements OffsetManager {

    private static final Logger log = LoggerFactory.getLogger(BrokerOffsetManager.class);

    private ConcurrentHashMap<MessageQueue, AtomicLong> offsetTable = new ConcurrentHashMap<>();
    private ScheduledExecutorService persistOffsetTimer;
    private ClientInstance clientInstance;
    private String group;

    public BrokerOffsetManager(ClientInstance clientInstance, String group) {
        this.clientInstance = clientInstance;
        this.group = group;
        this.persistOffsetTimer = new ScheduledThreadPoolExecutor(1);
    }
    @Override
    public void start() {
        this.persistOffsetTimer.scheduleAtFixedRate(() -> {
            persistOffset();
        }, 1000, 5 * 1000, TimeUnit.MILLISECONDS);
    }
    @Override
    public void record(MessageQueue messageQueue, long offset, String group) {

        AtomicLong localOff = offsetTable.get(messageQueue);
        if (localOff == null) {
            localOff = new AtomicLong(offset);
            offsetTable.put(messageQueue, localOff);
        }
        log.info("Local offset is {}, will be set in {}", localOff.get(), offset);
        localOff.set(offset);
    }
    @Override
    public long readOffset(MessageQueue messageQueue, String group) {
        AtomicLong localOff = offsetTable.get(messageQueue);
        if (localOff == null) {
            return -1;
        }
        return localOff.get();
    }
    @Override
    public void resetLocalOffset(String group, Map<MessageQueue, Long> allOffsets) {
        for (Map.Entry<MessageQueue, Long> entry : allOffsets.entrySet()) {
            MessageQueue queue = entry.getKey();
            long offset = entry.getValue();
            AtomicLong origin = offsetTable.get(queue);
            if (origin == null) {
                origin = new AtomicLong(offset);
                offsetTable.put(queue, origin);
            } else {
                if (origin.get() < offset) {
                    origin.set(offset);

                }
            }
        }


    }

    @Override
    public void persistOffset() {

        if (offsetTable == null || offsetTable.isEmpty()) {
            return;
        }
        Header header = new Header(RequestType.RECORD_OFFSET, RpcType.ONE_WAY, TopicUtil.generateUniqueID());
        PayLoad payLoad = new MQRecordOffsetRequest(offsetTable, group);
        RemoteCommand remoteCommand = new RemoteCommand(header, payLoad);
        Set<String> names = offsetTable.keySet().stream().map(MessageQueue::getBrokerName).collect(Collectors.toSet());
        this.clientInstance.sendOffsetToBroker(remoteCommand, names);
    }
}
