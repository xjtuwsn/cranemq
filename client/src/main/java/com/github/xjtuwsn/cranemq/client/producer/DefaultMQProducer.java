package com.github.xjtuwsn.cranemq.client.producer;

import cn.hutool.core.util.StrUtil;
import com.github.xjtuwsn.cranemq.client.producer.balance.LoadBalanceStrategy;
import com.github.xjtuwsn.cranemq.client.producer.result.SendResult;
import com.github.xjtuwsn.cranemq.common.remote.enums.RegistryType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.xjtuwsn.cranemq.common.remote.RemoteHook;
import com.github.xjtuwsn.cranemq.client.hook.SendCallback;
import com.github.xjtuwsn.cranemq.client.producer.impl.DefaultMQProducerImpl;
import com.github.xjtuwsn.cranemq.common.constant.MQConstant;
import com.github.xjtuwsn.cranemq.common.entity.Message;
import com.github.xjtuwsn.cranemq.common.exception.CraneClientException;
import com.github.xjtuwsn.cranemq.common.remote.RemoteAddress;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @project:cranemq
 * @file:DefaultMQProducer
 * @author:wsn
 * @create:2023/09/27-11:11
 * 消息生产者
 */
public class DefaultMQProducer implements MQProducer {
    private static final Logger log = LoggerFactory.getLogger(DefaultMQProducer.class);
    private String topic = MQConstant.DEFAULT_TOPIC_NAME;

    private String tag;
    private String group = MQConstant.DEFAULT_GROUP_NAME;

    private int createQueueNumber = MQConstant.DEFAULT_QUEUE_NUMBER;


    private RemoteAddress brokerAddress;
    private String registryAddr;
    // 响应超时时间，ms
    private long responseTimeoutMills = MQConstant.RESPONSE_TIMEOUT_MILLS;
    // 默认重试次数
    private int maxRetryTime = MQConstant.MAX_RETRY_TIMES;
    private DefaultMQProducerImpl defaultMQProducerImpl;
    private LoadBalanceStrategy loadBalanceStrategy;
    private RegistryType registryType;

    public DefaultMQProducer(String group) {
        this(group, null, null);
    }
    public DefaultMQProducer(String group, RemoteHook hook) {
        this(group, hook, null);
    }
    public DefaultMQProducer(String group, RemoteHook hook, String registryAddr) {
        this.group = group;
        this.registryAddr = registryAddr;
        this.defaultMQProducerImpl = new DefaultMQProducerImpl(this, hook, this.registryAddr);
    }
    @Override
    public void start() throws CraneClientException {
        if (StrUtil.isEmpty(this.registryAddr)) {
            throw new CraneClientException("Registry address can not be null or empty");
        }
        if (this.loadBalanceStrategy != null) {
            this.defaultMQProducerImpl.setLoadBalanceStrategy(this.loadBalanceStrategy);
        }
        this.defaultMQProducerImpl.setRegistryAddress(this.registryAddr);
        this.defaultMQProducerImpl.setRegistryType(this.registryType);
        this.defaultMQProducerImpl.start();
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

    @Override
    public void shutdown() {
        this.defaultMQProducerImpl.close();
    }

    /**
     * 同步发送单条
     * @param message
     * @return
     * @throws CraneClientException
     */

    @Override
    public SendResult send(Message message) throws CraneClientException {

        return this.defaultMQProducerImpl.sendSync(this.responseTimeoutMills, false, message);
    }

    /**
     * 单向发送单条
     * @param message
     * @param oneWay
     * @throws CraneClientException
     */
    @Override
    public void send(Message message, boolean oneWay) throws CraneClientException {
        if (!oneWay) {
            throw new CraneClientException("Call oneWay Method but flag is false");
        }
        this.defaultMQProducerImpl.sendSync(this.responseTimeoutMills, true, message);
    }

    /**
     * 异步发送单条消息
     * @param message
     * @param callback
     * @return
     * @throws CraneClientException
     */
    @Override
    public void send(Message message, SendCallback callback) throws CraneClientException {
        this.defaultMQProducerImpl.sendAsync(callback, this.responseTimeoutMills, message);
    }

    @Override
    public SendResult send(Message message, long delay, TimeUnit unit) {
        if (delay < 0) {
            throw new CraneClientException("Delay time can not be negtive");
        }
        if (unit == null) {
            throw new CraneClientException("Time unit can not be null");
        }
        long mills = Math.max(0, unit.toMillis(delay) - 1000);
        long second = TimeUnit.MILLISECONDS.toSeconds(mills);
        return this.defaultMQProducerImpl.sendSync(this.responseTimeoutMills, false, null, null, second, message);
    }

    /**
     * 同步批量消息
     * @param messages
     * @return
     * @throws CraneClientException
     */
    @Override
    public SendResult send(List<Message> messages) throws CraneClientException {
        return this.defaultMQProducerImpl.sendSync(this.responseTimeoutMills,
                false, messages.toArray(new Message[0]));
    }

    /**
     * 单向批量消息
     * @param messages
     * @param oneWay
     * @throws CraneClientException
     */
    @Override
    public void send(List<Message> messages, boolean oneWay) throws CraneClientException {
        if (!oneWay) {
            throw new CraneClientException("Call oneWay Method but flag is false");
        }
        this.defaultMQProducerImpl.sendSync(this.responseTimeoutMills,
                true, messages.toArray(new Message[0]));
    }

    /**
     * 异步批量消息
     * @param messages
     * @param callback
     * @throws CraneClientException
     */
    @Override
    public void send(List<Message> messages, SendCallback callback) throws CraneClientException {
        this.defaultMQProducerImpl.sendAsync(callback, this.responseTimeoutMills, messages.toArray(new Message[0]));
    }

    @Override
    public SendResult send(Message message, MQSelector selector, Object arg) {
        return this.defaultMQProducerImpl.sendSync(this.responseTimeoutMills, false, selector, arg, 0, message);
    }

    public void bindRegistry(String registryAddr, RegistryType registryType) {
        if (StrUtil.isEmpty(registryAddr)) {
            throw new CraneClientException("Registery address canot be null or empty");
        }
        this.registryAddr = registryAddr;
        this.registryType = registryType;
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

    public String getRegistryAddr() {
        return registryAddr;
    }

    public void setLoadBalanceStrategy(LoadBalanceStrategy loadBalanceStrategy) {
        this.loadBalanceStrategy = loadBalanceStrategy;
    }

    public RegistryType getRegistryType() {
        return registryType;
    }
}
