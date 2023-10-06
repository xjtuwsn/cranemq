package com.github.xjtuwsn.cranemq.client.producer;

import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;

import java.util.List;

/**
 * @project:cranemq
 * @file:MQSelector
 * @author:wsn
 * @create:2023/10/06-20:22
 */
public interface MQSelector {

    MessageQueue select(List<MessageQueue> queues, Object arg);
}
