package com.github.xjtuwsn.cranemq.client.consumer.offset;

import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @project:cranemq
 * @file:BrokerOffsetManager
 * @author:wsn
 * @create:2023/10/10-15:12
 */
public class BrokerOffsetManager implements OffsetManager {

    private static final Logger log = LoggerFactory.getLogger(BrokerOffsetManager.class);

    private ConcurrentHashMap<MessageQueue, AtomicLong> offsetTable = new ConcurrentHashMap<>();
    @Override
    public void record(MessageQueue messageQueue, long offset) {

        AtomicLong localOff = offsetTable.get(messageQueue);
        if (localOff == null) {
            localOff = new AtomicLong(offset);
            offsetTable.put(messageQueue, localOff);
        }
        localOff.set(offset);
    }
    @Override
    public long readOffset(MessageQueue messageQueue) {
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
                origin.set(offset);
            }
        }

        System.out.println("OffsetTable: " + this.offsetTable);

    }
}
