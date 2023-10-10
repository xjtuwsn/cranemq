package com.github.xjtuwsn.cranemq.client.consumer;

import com.github.xjtuwsn.cranemq.client.consumer.impl.DefaultPushConsumerImpl;
import com.github.xjtuwsn.cranemq.client.consumer.listener.CommonMessageListener;
import com.github.xjtuwsn.cranemq.client.consumer.listener.MessageListener;
import com.github.xjtuwsn.cranemq.client.consumer.listener.OrderedMessageListener;
import com.github.xjtuwsn.cranemq.common.constant.MQConstant;
import com.github.xjtuwsn.cranemq.common.consumer.MessageModel;
import com.github.xjtuwsn.cranemq.common.consumer.StartConsume;
import com.github.xjtuwsn.cranemq.common.exception.CraneClientException;
import com.github.xjtuwsn.cranemq.common.remote.RemoteHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @project:cranemq
 * @file:DefaultPushConsumer
 * @author:wsn
 * @create:2023/10/08-10:49
 */
public class DefaultPushConsumer implements MQPushConsumer {
    private static final Logger log = LoggerFactory.getLogger(DefaultPushConsumer.class);
    private String consumerGroup = MQConstant.DEFAULT_CONSUMER_GROUP;
    private MessageModel messageModel = MessageModel.CLUSTER;
    private StartConsume startConsume = StartConsume.FROM_LAST_OFFSET;

    private MessageListener messageListener;
    private DefaultPushConsumerImpl defaultPushConsumer;
    public DefaultPushConsumer() {
        this(MQConstant.DEFAULT_CONSUMER_GROUP);
    }
    public DefaultPushConsumer(String consumerGroup) {
        this(consumerGroup, null);
    }
    public DefaultPushConsumer(String consumerGroup, RemoteHook hook) {
        this.consumerGroup = consumerGroup;
        this.defaultPushConsumer = new DefaultPushConsumerImpl(this, hook);
    }
    @Override
    public void subscribe(String topic, String tags) {
        if (topic == null || tags == null) {
            throw new CraneClientException("Topic or Tags cannot be null");
        }
        this.defaultPushConsumer.subscribe(topic, tags);
    }

    @Override
    public void bindRegistry(String address) {
        if (address == null) {
            throw new CraneClientException("Address cannot be null");
        }
        this.defaultPushConsumer.bindRegistry(address);
    }

    @Override
    public void start() {
        if (messageListener == null) {
            throw new CraneClientException("Message listener can not be null");
        }
        this.defaultPushConsumer.start();
    }

    @Override
    public void shutdown() {
        this.defaultPushConsumer.shutdown();
    }

    @Override
    public void setStartFrom(StartConsume startConsume) {
        if (startConsume == null) {
            throw new CraneClientException("StartConsume cannot be null");
        }
        this.startConsume = startConsume;
    }

    @Override
    public void setMesageModel(MessageModel messageModel) {
        if (messageModel == null) {
            throw new CraneClientException("MessageModel cannot be null");
        }
        this.messageModel = messageModel;
    }

    @Override
    public void registerListener(MessageListener messageListener) {
        if (messageListener == null) {
            throw new CraneClientException("MessageListener cannot be null");
        }
        this.messageListener = messageListener;
    }

    @Override
    public void registerListener(CommonMessageListener commonMessageListener) {
        if (commonMessageListener == null) {
            throw new CraneClientException("CommonMessageListener cannot be null");
        }
        this.messageListener = commonMessageListener;
    }

    @Override
    public void registerListener(OrderedMessageListener orderedMessageListener) {
        if (orderedMessageListener == null) {
            throw new CraneClientException("OrderedMessageListener cannot be null");
        }
        this.messageListener = orderedMessageListener;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public StartConsume getStartConsume() {
        return startConsume;
    }

    public MessageListener getMessageListener() {
        return messageListener;
    }
}
