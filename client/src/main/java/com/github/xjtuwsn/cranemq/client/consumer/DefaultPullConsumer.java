package com.github.xjtuwsn.cranemq.client.consumer;

import com.github.xjtuwsn.cranemq.client.consumer.impl.DefaultPullConsumerImpl;
import com.github.xjtuwsn.cranemq.common.constant.MQConstant;
import com.github.xjtuwsn.cranemq.common.consumer.MessageModel;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.github.xjtuwsn.cranemq.common.exception.CraneClientException;
import com.github.xjtuwsn.cranemq.common.remote.RemoteHook;

import java.util.List;

/**
 * @project:cranemq
 * @file:DefaultPullConsumer
 * @author:wsn
 * @create:2023/10/07-10:37
 */
public class DefaultPullConsumer implements MQPullConsumer {
    private String consumerGroup = MQConstant.DEFAULT_CONSUMER_GROUP;
    private long maxTimeoutMills = MQConstant.MAX_PULL_TIMEOUT_MILLS;
    private int maxPullLength = 30;
    private MessageModel messageModel = MessageModel.BRODERCAST;
    private int maxRetryTime = 5;
    private RemoteHook hook;
    private DefaultPullConsumerImpl defaultPullConsumerImpl;


    public DefaultPullConsumer(String consumerGroup) {
        this(consumerGroup, null);
    }

    public DefaultPullConsumer(String consumerGroup, RemoteHook hook) {
        this.consumerGroup = consumerGroup;
        this.hook = hook;
        this.defaultPullConsumerImpl = new DefaultPullConsumerImpl(this, hook);
    }

    @Override
    public void setId(String id) {

    }

    @Override
    public void subscribe(String topic, String tags) {
        this.defaultPullConsumerImpl.subscribe(topic, tags);
    }

    @Override
    public void bindRegistry(String address) {
        this.defaultPullConsumerImpl.bindRegistry(address);
    }

    @Override
    public PullResult pull(MessageQueue messageQueue, long offset, int len) throws CraneClientException {
        return this.defaultPullConsumerImpl.pull(messageQueue, offset, len);
    }

    @Override
    public List<MessageQueue> listQueues() throws CraneClientException {
        return this.defaultPullConsumerImpl.listQueues();
    }

    @Override
    public List<MessageQueue> lisrQueues(String topic) throws CraneClientException {
        return this.defaultPullConsumerImpl.listQueues(topic);
    }
    @Override
    public void start() {
        this.defaultPullConsumerImpl.start();
    }

    @Override
    public void shutdown() {
        this.defaultPullConsumerImpl.shutdown();
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public long getMaxTimeoutMills() {
        return maxTimeoutMills;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public int getMaxPullLength() {
        return maxPullLength;
    }

    public int getMaxRetryTime() {
        return maxRetryTime;
    }
}
