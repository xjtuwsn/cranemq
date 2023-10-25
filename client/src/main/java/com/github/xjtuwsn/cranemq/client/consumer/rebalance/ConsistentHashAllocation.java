package com.github.xjtuwsn.cranemq.client.consumer.rebalance;

import cn.hutool.core.lang.Pair;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @project:cranemq
 * @file:ConsistentHashAllocation
 * @author:wsn
 * @create:2023/10/14-10:24
 * 一致性哈希策略
 */
public class ConsistentHashAllocation implements QueueAllocation {
    private static final Logger log = LoggerFactory.getLogger(ConsistentHashAllocation.class);
    private static final int DUMMY_NUMBER = 200;

    private static final int MOD = Integer.MAX_VALUE;

    private TreeMap<Integer, Pair<String, Integer>> hashRing;
    @Override
    public List<MessageQueue> allocate(List<MessageQueue> queues, Set<String> groupMember, String self) {
        hashRing = new TreeMap<>();
        for (String memeber: groupMember) {
            addNode(memeber);
        }
        List<MessageQueue> result = new ArrayList<>();
        for (MessageQueue messageQueue : queues) {
            int hash = hash(messageQueue);
            NavigableMap<Integer, Pair<String, Integer>> tailMap = hashRing.tailMap(hash, true);
            Pair<String, Integer> current = null;
            if (tailMap.isEmpty()) {
                current = hashRing.get(hashRing.firstKey());
            } else {
                current = tailMap.get(tailMap.firstKey());
            }
            if (current.getKey().equals(self)) {
                result.add(messageQueue);
            }
        }
        return result;

    }

    private void addNode(String groupName) {
        for (int i = 0; i < DUMMY_NUMBER; i++) {
            int hash = hash(groupName, i);
            hashRing.put(hash, new Pair<>(groupName, i));
        }
    }

    private int hash(String memeber, int index) {
        byte[] data = (memeber + "--" + index).getBytes();
        HashCode hashCode = Hashing.murmur3_128().hashBytes(data);
        return (int) (hashCode.asLong() % MOD);
    }
    private int hash(MessageQueue queue) {
        byte[] data = (queue.getTopic() + "--" + queue.getBrokerName() + "--" + queue.getQueueId()).getBytes();
        HashCode hashCode = Hashing.murmur3_128().hashBytes(data);
        return (int) (hashCode.asLong() % MOD);
    }
}
