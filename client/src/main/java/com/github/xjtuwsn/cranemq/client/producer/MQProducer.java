package com.github.xjtuwsn.cranemq.client.producer;

import com.github.xjtuwsn.cranemq.client.hook.SendCallback;
import com.github.xjtuwsn.cranemq.client.producer.result.SendResult;
import com.github.xjtuwsn.cranemq.common.entity.Message;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @project:cranemq
 * @file:MQProducer
 * @author:wsn
 * @create:2023/09/27-11:10
 */
public interface MQProducer {
    void start();

    void shutdown();
    SendResult send(Message message);
    void send(Message message, boolean oneWay);
    void send(Message message, SendCallback callback);

    SendResult send(Message message, long delay, TimeUnit unit);

    SendResult send(List<Message> messages);
    void send(List<Message> messages, boolean oneWay);
    void send(List<Message> messages, SendCallback callback);

    SendResult send(Message message, MQSelector selector, Object arg);
}
