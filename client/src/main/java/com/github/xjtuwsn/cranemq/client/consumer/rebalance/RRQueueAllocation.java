package com.github.xjtuwsn.cranemq.client.consumer.rebalance;

import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @project:cranemq
 * @file:RRQueueAllocation
 * @author:wsn
 * @create:2023/10/14-10:19
 * 轮询策略
 */
public class RRQueueAllocation implements QueueAllocation {
    @Override
    public List<MessageQueue> allocate(List<MessageQueue> queues, Set<String> groupMember, String self) {
        List<String> memebers = new ArrayList<>(groupMember);
        memebers.sort(String::compareTo);
        List<MessageQueue> result = new ArrayList<>();

        int number = memebers.size();
        for (int i = 0; i < queues.size(); i++) {
            MessageQueue queue = queues.get(i);
            int index = i % number;
            if (memebers.get(index).equals(self)) {
                result.add(queue);
            }
        }
        return result;
    }
}
