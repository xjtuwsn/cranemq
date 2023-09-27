package com.github.xjtuwsn.cranemq.client.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.xjtuwsn.cranemq.broker.core.MessageQueue;
import com.github.xjtuwsn.cranemq.client.hook.RemoteHook;
import com.github.xjtuwsn.cranemq.client.hook.SendCallback;
import com.github.xjtuwsn.cranemq.client.producer.impl.DefaultMQProducerImpl;
import com.github.xjtuwsn.cranemq.common.constant.ProducerConstant;
import com.github.xjtuwsn.cranemq.common.entity.Message;
import com.github.xjtuwsn.cranemq.common.exception.CraneClientException;
import com.github.xjtuwsn.cranemq.common.net.RemoteAddress;

import java.util.List;

/**
 * @project:cranemq
 * @file:DefaultMQProducer
 * @author:wsn
 * @create:2023/09/27-11:11
 */
public class DefaultMQProducer implements MQProducer {
    private static final Logger log = LoggerFactory.getLogger(DefaultMQProducer.class);
    private String topic = ProducerConstant.DEFAULT_TOPIC_NAME;

    private String tag;
    private String group = ProducerConstant.DEFAULT_GROUP_NAME;

    private int createQueueNumber = ProducerConstant.DEFAULT_QUEUE_NUMBER;

    private List<MessageQueue> availableQueue;

    private RemoteAddress brokerAddress;
    // 响应超时时间，ms
    private long responseTimeoutMills = ProducerConstant.RESPONSE_TIMEOUT_MILLS;
    // 默认重试次数
    private int maxRetryTime = ProducerConstant.MAX_RETRY_TIMES;
    private DefaultMQProducerImpl defaultMQProducerImpl;

    public DefaultMQProducer(String group) {
        this(group, null);
    }
    public DefaultMQProducer(String group, RemoteHook hook) {
        this.group = group;
        this.defaultMQProducerImpl = new DefaultMQProducerImpl(this, hook);
    }
    @Override
    public void start() throws CraneClientException {
        this.defaultMQProducerImpl.start();
    }

    @Override
    public void shutdown() {
        this.defaultMQProducerImpl.close();
    }

    @Override
    public SendResult send(Message message) {
        if (message == null) {
            throw new CraneClientException("Message cannot be null");
        }
        this.defaultMQProducerImpl.send(message);
        return null;
    }

    @Override
    public void send(Message message, boolean oneWay) {

    }

    @Override
    public SendResult send(Message message, SendCallback callback) {
        return null;
    }

    @Override
    public SendResult send(List<Message> messages) {
        return null;
    }

    @Override
    public void send(List<Message> messages, boolean oneWay) {

    }

    @Override
    public SendCallback send(List<Message> messages, SendCallback callback) {
        return null;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public int getCreateQueueNumber() {
        return createQueueNumber;
    }

    public void setCreateQueueNumber(int createQueueNumber) {
        this.createQueueNumber = createQueueNumber;
    }

    public RemoteAddress getBrokerAddress() {
        return brokerAddress;
    }

    public void setBrokerAddress(RemoteAddress brokerAddress) {
        this.brokerAddress = brokerAddress;
    }

    public long getResponseTimeoutMills() {
        return responseTimeoutMills;
    }

    public void setResponseTimeoutMills(long responseTimeoutMills) {
        this.responseTimeoutMills = responseTimeoutMills;
    }

    public int getMaxRetryTime() {
        return maxRetryTime;
    }

    public void setMaxRetryTime(int maxRetryTime) {
        this.maxRetryTime = maxRetryTime;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }
}
