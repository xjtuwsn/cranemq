package com.github.xjtuwsn.cranemq.client.consumer;

import com.github.xjtuwsn.cranemq.client.consumer.listener.CommonMessageListener;
import com.github.xjtuwsn.cranemq.client.consumer.listener.MessageListener;
import com.github.xjtuwsn.cranemq.client.consumer.listener.OrderedMessageListener;

/**
 * @project:cranemq
 * @file:MQPushConsumer
 * @author:wsn
 * @create:2023/10/06-22:18
 */
public interface MQPushConsumer extends MQConsumer {

    void registerListener(MessageListener messageListener);

    void registerListener(CommonMessageListener commonMessageListener);

    void registerListener(OrderedMessageListener orderedMessageListener);
}
