package com.github.xjtuwsn.cranemq.client.consumer.push;

import com.github.xjtuwsn.cranemq.common.command.payloads.resp.MQPullMessageResponse;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.github.xjtuwsn.cranemq.common.entity.ReadyMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @project:cranemq
 * @file:UnConsumedQueue
 * @author:wsn
 * @create:2023/10/08-15:32
 */
public interface ConsumeMessageService {
    public static final int COUSMER_CORE_SIZE = 4;
    public static final int COUSMER_MAX_SIZE = 8;
    void start();

    void shutdown();

    void submit(MessageQueue messageQueue, BrokerQueueSnapShot snapShot, List<ReadyMessage> messages);


}
