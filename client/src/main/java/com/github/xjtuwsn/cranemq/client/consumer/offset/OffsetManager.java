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

    void record(MessageQueue messageQueue, long offset);

    long readOffset(MessageQueue messageQueue);
    void resetLocalOffset(String group, Map<MessageQueue, Long> allOffsets);
}
