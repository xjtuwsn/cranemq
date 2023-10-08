package com.github.xjtuwsn.cranemq.client.consumer.push;

import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.command.payloads.resp.MQPushMessageResponse;
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
public class UnConsumedQueue {

    private static final Logger log = LoggerFactory.getLogger(UnConsumedQueue.class);

    private LinkedBlockingQueue<MQPushMessageResponse> responseQueue = new LinkedBlockingQueue<>(8000);

    private AtomicInteger messageCount;

    public MQPushMessageResponse take() {
        try {
            MQPushMessageResponse take = responseQueue.take();
            messageCount.addAndGet(-take.getMessageCount());
            return take;
        } catch (InterruptedException e) {
            log.warn("Take has been Interrupted");
        }
        return null;
    }

    public void offer(MQPushMessageResponse mqPushMessageResponse) {
        if (mqPushMessageResponse == null) {
            log.warn("MQPushMessageResponse can not be null");
            return;
        }
        messageCount.addAndGet(mqPushMessageResponse.getMessageCount());
        responseQueue.offer(mqPushMessageResponse);
    }
}
