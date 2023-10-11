package com.github.xjtuwsn.cranemq.client.consumer.offset;

import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;

import java.util.Map;

/**
 * @project:cranemq
 * @file:OffsetManager
 * @author:wsn
 * @create:2023/10/10-15:37
 */
public interface OffsetManager {
    void start();

    void record(MessageQueue messageQueue, long offset, String group);

    long readOffset(MessageQueue messageQueue, String group);
    void resetLocalOffset(String group, Map<MessageQueue, Long> allOffsets);

    void persistOffset();
}
