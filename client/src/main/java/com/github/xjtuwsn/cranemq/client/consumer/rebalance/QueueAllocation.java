package com.github.xjtuwsn.cranemq.client.consumer.rebalance;

import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;

import java.util.List;
import java.util.Set;

/**
 * @project:cranemq
 * @file:QueueAllocation
 * @author:wsn
 * @create:2023/10/08-16:15
 */
public interface QueueAllocation {

    List<MessageQueue> allocate(List<MessageQueue> queues, Set<String> groupMember, String self);
}
