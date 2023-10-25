package com.github.xjtuwsn.cranemq.client.consumer.rebalance;

import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * @project:cranemq
 * @file:AverageQueueAllocation
 * @author:wsn
 * @create:2023/10/08-16:17
 * 平均分配策略
 */
public class AverageQueueAllocation implements QueueAllocation {
    @Override
    public List<MessageQueue> allocate(List<MessageQueue> queues, Set<String> groupMember, String self) {
        List<String> memebers = new ArrayList<>(groupMember);
        memebers.sort(String::compareTo);
        List<MessageQueue> result = new ArrayList<>();
        int queueSize = queues.size(), all = memebers.size();
        int div = queueSize / all, left = queueSize % all, done = 0;
        for (int i = 0; i < all; i++) {
            String cur = memebers.get(i);
            if (cur.equals(self)) {
                int limit = done + div;
                if (i < left) {
                    limit++;
                }
                for (int j = done; j < limit; j++) {
                    result.add(queues.get(j));
                }
            }
            done += (i < left ? div + 1 : div);
        }
        return result;
    }
}
