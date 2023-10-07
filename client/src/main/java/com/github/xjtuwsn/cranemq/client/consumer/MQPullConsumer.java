package com.github.xjtuwsn.cranemq.client.consumer;

import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.github.xjtuwsn.cranemq.common.exception.CraneClientException;

import java.util.List;

/**
 * @project:cranemq
 * @file:MQPullConsumer
 * @author:wsn
 * @create:2023/10/06-22:18
 */
public interface MQPullConsumer extends MQConsumer {

    PullResult pull(MessageQueue messageQueue, long offset, int len) throws CraneClientException;
    List<MessageQueue> listQueues() throws CraneClientException;
    List<MessageQueue> lisrQueues(String topic) throws CraneClientException;
}
