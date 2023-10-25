package com.github.xjtuwsn.cranemq.client.producer;

import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.github.xjtuwsn.cranemq.common.exception.CraneClientException;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @project:cranemq
 * @file:MQSelectorGray
 * @author:wsn
 * @create:2023/10/23-10:12
 * 灰度队列选择
 */
public abstract class MQSelectorGray implements MQSelector {
    private int queueNum;
    private AtomicLong count;
    public MQSelectorGray(int queueNum) {
        this.queueNum = queueNum;
        this.count = new AtomicLong(0);
    }

    /**
     * 根据给定的灰度队列数量，轮询队列中的前n个队列
     * @param queues
     * @param arg
     * @return
     */
    @Override
    public MessageQueue select(List<MessageQueue> queues, Object arg) {
        int size = queues.size();
        if (queueNum > size) {
            throw new CraneClientException("Gray queue picked more than origin queue");
        }
        MessageQueue queue = queues.get((int) (count.get() % queueNum));
        count.getAndIncrement();
        return queue;
    }
}
