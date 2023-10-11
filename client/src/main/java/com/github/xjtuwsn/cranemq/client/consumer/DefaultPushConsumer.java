package com.github.xjtuwsn.cranemq.client.consumer;

import cn.hutool.core.lang.Pair;
import cn.hutool.core.util.StrUtil;
import com.github.xjtuwsn.cranemq.client.consumer.impl.DefaultPushConsumerImpl;
import com.github.xjtuwsn.cranemq.client.consumer.listener.CommonMessageListener;
import com.github.xjtuwsn.cranemq.client.consumer.listener.MessageListener;
import com.github.xjtuwsn.cranemq.client.consumer.listener.OrderedMessageListener;
import com.github.xjtuwsn.cranemq.common.constant.MQConstant;
import com.github.xjtuwsn.cranemq.common.consumer.MessageModel;
import com.github.xjtuwsn.cranemq.common.consumer.StartConsume;
import com.github.xjtuwsn.cranemq.common.exception.CraneClientException;
import com.github.xjtuwsn.cranemq.common.remote.RemoteHook;
import lombok.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
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
    private String id = "0";
    private RemoteHook hook;
    public DefaultPushConsumer() {
        this(MQConstant.DEFAULT_CONSUMER_GROUP);
    }
    public DefaultPushConsumer(String consumerGroup) {
        this(consumerGroup, null);
    }
    public DefaultPushConsumer(String consumerGroup, RemoteHook hook) {
        this.consumerGroup = consumerGroup;
        this.hook = hook;
        this.defaultPushConsumer = new DefaultPushConsumerImpl(this, hook);
    }

    public DefaultPushConsumer(String consumerGroup, MessageModel messageModel, StartConsume startConsume,
                               MessageListener messageListener, String id, RemoteHook hook, String address,
                               List<Pair<String, String>> topics) {
        if (StrUtil.isEmpty(consumerGroup) || messageModel == null || startConsume == null
                || messageListener == null || StrUtil.isEmpty(id) || StrUtil.isEmpty(address)) {
            throw new CraneClientException("Paramters error");
        }
        this.consumerGroup = consumerGroup;
        this.messageModel = messageModel;
        this.startConsume = startConsume;
        this.messageListener = messageListener;
        this.id = id;
        this.hook = hook;
        this.defaultPushConsumer = new DefaultPushConsumerImpl(this, hook, address, topics);
    }
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void setId(String id) {
        if (StrUtil.isEmpty(id)) {
            throw new CraneClientException("ID canont be empty");
        }
        this.id = id;
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

    public String getId() {
        return id;
    }
    public static class Builder {
        private String consumerGroup = MQConstant.DEFAULT_CONSUMER_GROUP;
        private MessageModel messageModel = MessageModel.CLUSTER;
        private StartConsume startConsume = StartConsume.FROM_LAST_OFFSET;

        private MessageListener messageListener;
        private String id = "0";
        private RemoteHook hook;
        private String registerAddress;
        private List<Pair<String, String>> topics;

        public Builder() {
            this.topics = new ArrayList<>();
        }

        public Builder consumerGroup(String consumerGroup) {
            this.consumerGroup = consumerGroup;
            return this;
        }
        public Builder messageModel(MessageModel messageModel) {
            this.messageModel = messageModel;
            return this;
        }
        public Builder startConsume(StartConsume startConsume) {
            this.startConsume = startConsume;
            return this;
        }
        public Builder messageListener(MessageListener messageListener) {
            this.messageListener = messageListener;
            return this;
        }
        public Builder consumerId(String id) {
            this.id = id;
            return this;
        }
        public Builder remoteHook(RemoteHook hook) {
            this.hook = hook;
            return this;
        }
        public Builder bindRegistry(String address) {
            this.registerAddress = address;
            return this;
        }
        public Builder subscribe(String topic, String tag) {
            if (StrUtil.isEmpty(topic)) {
                throw new CraneClientException("Topic cannot be null");
            }
            this.topics.add(new Pair<>(topic, tag));
            return this;
        }

        public DefaultPushConsumer build() {
            return new DefaultPushConsumer(consumerGroup, messageModel, startConsume, messageListener, id, hook,
                    registerAddress, topics);
        }
    }
}
