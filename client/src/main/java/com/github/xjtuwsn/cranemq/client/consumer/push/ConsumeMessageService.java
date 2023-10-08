package com.github.xjtuwsn.cranemq.client.consumer.push;

import com.github.xjtuwsn.cranemq.common.command.payloads.resp.MQPullMessageResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @project:cranemq
 * @file:UnConsumedQueue
 * @author:wsn
 * @create:2023/10/08-15:32
 */
public class ConsumeMessageService {

    private static final Logger log = LoggerFactory.getLogger(ConsumeMessageService.class);
    private int capacity = 8000;

    private LinkedBlockingQueue<MQPullMessageResponse> responseQueue = new LinkedBlockingQueue<>(8000);

    private AtomicInteger messageCount;

    public MQPullMessageResponse take() {
        try {
            MQPullMessageResponse take = responseQueue.take();
            messageCount.addAndGet(-take.getMessageCount());
            return take;
        } catch (InterruptedException e) {
            log.warn("Take has been Interrupted");
        }
        return null;
    }

    public void offer(MQPullMessageResponse mqPullMessageResponse) {
        if (mqPullMessageResponse == null) {
            log.warn("MQPushMessageResponse can not be null");
            return;
        }
        messageCount.addAndGet(mqPullMessageResponse.getMessageCount());
        responseQueue.offer(mqPullMessageResponse);
    }
    public double getUsageRate() {
        if (messageCount.get() > 10000) {
            return 10;
        }
        if (responseQueue.size() < capacity / 2) {
            return 0;
        }
        return responseQueue.size() * 10.0 / capacity;
    }
}
