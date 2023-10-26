package com.github.xjtuwsn.cranemq.client.consumer.rebalance;

import com.github.xjtuwsn.cranemq.common.constant.MQConstant;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.github.xjtuwsn.cranemq.common.exception.CraneClientException;

import java.util.*;

/**
 * @project:cranemq
 * @file:GrayQueueAllocation
 * @author:wsn
 * @create:2023/10/23-10:28
 */

/**
 * 用于灰度环境下的队列分配策略，灰度客户端和正常客户端分开分配
 * @author wsn
 */
public class GrayQueueAllocation implements QueueAllocation {
    // 灰度队列的数量，与生产者设置保持一致
    private int grayQueueNum;

    // 区分完灰度和非灰度客户端之后的内部分配策略
    private QueueAllocation queueAllocation;

    public GrayQueueAllocation(int grayQueueNum) {
        this(grayQueueNum, new RRQueueAllocation());
    }
    public GrayQueueAllocation(int grayQueueNum, QueueAllocation queueAllocation) {
        this.grayQueueNum = grayQueueNum;
        if (queueAllocation instanceof GrayQueueAllocation) {
            throw new CraneClientException("Inner queue allocation can not be the same");
        }
        this.queueAllocation = queueAllocation;
    }

    /**
     * 进行灰度分配
     * @param queues
     * @param groupMember
     * @param self
     * @return
     */
    @Override
    public List<MessageQueue> allocate(List<MessageQueue> queues, Set<String> groupMember, String self) {

        Collections.sort(queues);
        List<MessageQueue> grayQueue = new ArrayList<>(), normalQueue = new ArrayList<>();
        Set<String> grayMemeber = new HashSet<>(), normalMember = new HashSet<>();
        // 提取灰度队列和正常队列
        for (int i = 0; i < queues.size(); i++) {
            if (i < grayQueueNum) {
                grayQueue.add(queues.get(i));
            } else {
                normalQueue.add(queues.get(i));
            }
        }
        // 灰度客户端和正常客户端
        for (String client : groupMember) {
            if (isGray(client)) {
                grayMemeber.add(client);
            } else {
                normalMember.add(client);
            }
        }
        // 针对当前客户端进行不同分配
        if (isGray(self)) {
            return queueAllocation.allocate(grayQueue, grayMemeber, self);
        }
        return queueAllocation.allocate(normalQueue, normalMember, self);
    }

    /**
     * 判断是否是灰度机
     * @param client
     * @return
     */
    private boolean isGray(String client) {
        int index = client.indexOf(MQConstant.GRAY_SUFFIX);
        return index != -1 && index == client.length() - MQConstant.GRAY_SUFFIX.length();
    }
}
