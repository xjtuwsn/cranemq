package com.github.xjtuwsn.cranemq.client.producer.impl;

import cn.hutool.core.collection.ConcurrentHashSet;
import cn.hutool.core.util.StrUtil;
import com.github.xjtuwsn.cranemq.client.remote.WrapperFutureCommand;
import com.github.xjtuwsn.cranemq.client.hook.SendCallback;
import com.github.xjtuwsn.cranemq.client.producer.MQSelector;
import com.github.xjtuwsn.cranemq.client.producer.balance.LoadBalanceStrategy;
import com.github.xjtuwsn.cranemq.client.producer.result.SendResult;
import com.github.xjtuwsn.cranemq.common.command.*;
import com.github.xjtuwsn.cranemq.common.command.payloads.req.MQBachProduceRequest;
import com.github.xjtuwsn.cranemq.common.remote.enums.RegistryType;
import com.github.xjtuwsn.cranemq.common.utils.TopicUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.xjtuwsn.cranemq.common.remote.RemoteHook;
import com.github.xjtuwsn.cranemq.client.producer.DefaultMQProducer;
import com.github.xjtuwsn.cranemq.client.producer.MQProducerInner;
import com.github.xjtuwsn.cranemq.client.remote.ClientFactory;
import com.github.xjtuwsn.cranemq.client.remote.ClientInstance;
import com.github.xjtuwsn.cranemq.common.command.payloads.req.MQProduceRequest;
import com.github.xjtuwsn.cranemq.common.command.types.RequestType;
import com.github.xjtuwsn.cranemq.common.command.types.RpcType;
import com.github.xjtuwsn.cranemq.common.entity.Message;
import com.github.xjtuwsn.cranemq.common.exception.CraneClientException;
import com.github.xjtuwsn.cranemq.common.remote.RemoteAddress;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @project:cranemq
 * @file:DefaultMQProducerImpl
 * @author:wsn
 * @create:2023/09/27-14:38
 * 消息生产者实现
 */
public class DefaultMQProducerImpl implements MQProducerInner {

    private static final Logger log = LoggerFactory.getLogger(DefaultMQProducer.class);

    private DefaultMQProducer defaultMQProducer;

    private RemoteHook hook;

    private ClientInstance clientInstance;
    private RemoteAddress address;
    private String registryAddress;
    private String clientID;
    private String id;
    private LoadBalanceStrategy loadBalanceStrategy;
    private RegistryType registryType;
    private ConcurrentHashSet<String> topicSet = new ConcurrentHashSet<>();
    /**
     * 0: created
     * 1: started
     * 2: failed
     */
    private volatile AtomicInteger state;
    public DefaultMQProducerImpl(DefaultMQProducer defaultMQProducer,
                                 RemoteHook hook, String registryAddress) {
        this(defaultMQProducer, hook, registryAddress, null);
    }


    public DefaultMQProducerImpl(DefaultMQProducer defaultMQProducer,
                                 RemoteHook hook, String registryAddress,
                                 LoadBalanceStrategy loadBalanceStrategy) {
        this.defaultMQProducer = defaultMQProducer;
        this.hook = hook;
        this.registryAddress = registryAddress;

        this.state = new AtomicInteger(0);
        this.loadBalanceStrategy = loadBalanceStrategy;
    }

    public void start() throws CraneClientException {
        if (this.state.get() != 0) {
            log.error("DefaultMQProducerImpl has already been started or faied!");
            throw new CraneClientException("DefaultMQProducerImpl has already been started or faied!");
        }
        this.checkConfig();
        this.address = this.defaultMQProducer.getBrokerAddress();
        this.clientID = TopicUtil.buildClientID("producer");

        this.clientInstance = ClientFactory.newInstance().getOrCreate(this.clientID, this.hook);
        if (this.loadBalanceStrategy != null) {
            this.clientInstance.setLoadBalanceStrategy(this.loadBalanceStrategy);
        }
        this.clientInstance.setRegistryType(registryType);
        this.clientInstance.registerHook(hook);
        id = this.clientInstance.registerProducer(this);
        this.state.set(1);
        this.clientInstance.start();


    }

    public SendResult sendSync(long timeout, boolean isOneWay, MQSelector selector, Object arg, long delay, Message... messages)
            throws CraneClientException {
        if (messages == null || messages.length == 0) {
            throw new CraneClientException("Message cannot be empty!");
        }
        WrapperFutureCommand wrappered = buildRequest(RpcType.SYNC, null, timeout, delay, messages);
        if (selector != null) {
            wrappered.setSelector(selector);
            wrappered.setArg(arg);
        }
        String correlationID = wrappered.getFutureCommand().getRequest().getHeader().getCorrelationId();
        if (isOneWay) {
            wrappered.getFutureCommand().getRequest().getHeader().setRpcType(RpcType.ONE_WAY);
        }
        if (wrappered == null) {
            throw new CraneClientException("Create Request error!");
        }
        FutureCommand futureCommand = wrappered.getFutureCommand();

        return this.clientInstance.sendMessageSync(wrappered, isOneWay);
    }

    public SendResult sendSync(long timeout, boolean isOneWay, Message... messages)
            throws CraneClientException {
        return this.sendSync(timeout, isOneWay, null, null, 0, messages);
    }

    public void sendAsync(SendCallback callback, long timeout, Message... messages) {
        if (messages == null || messages.length == 0) {
            throw new CraneClientException("Message cannot be empty!");
        }
        WrapperFutureCommand remoteCommand = buildRequest(RpcType.ASYNC, callback, timeout, 0, messages);
        if (remoteCommand == null) {
            throw new CraneClientException("Create Request error!");
        }

        this.clientInstance.sendMessageAsync(remoteCommand);
    }


    /**
     * TODO 未添加响应消息处理，超时错误Failure设置
     * @param rpcType
     * @param callback
     * @param timeout
     * @param messages
     * @return
     */
    private WrapperFutureCommand buildRequest(RpcType rpcType, SendCallback callback, long timeout, long delay, Message... messages) {
        String topic = messages[0].getTopic();
        if (StrUtil.isEmpty(topic)) {
            throw new CraneClientException("Topic cannot be null");
        }
        if (!TopicUtil.checkTopic(topic)) {
            throw new CraneClientException("Topic name is invalid!");
        }
        this.topicSet.add(topic);
        String correlationID = TopicUtil.generateUniqueID();
        Header header = new Header(RequestType.MESSAGE_PRODUCE_REQUEST, rpcType, correlationID);
        if (messages.length > 1) {
            header.setCommandType(RequestType.MESSAGE_BATCH_PRODUCE_REAUEST);
        }
        PayLoad payLoad = null;
        if (messages.length == 1) {
            if (delay > 0) {
                header.setCommandType(RequestType.DELAY_MESSAGE_PRODUCE_REQUEST);
                payLoad = new MQProduceRequest(messages[0], delay);
            } else {
                payLoad = new MQProduceRequest(messages[0]);
            }
        } else {
            payLoad = new MQBachProduceRequest(Arrays.asList(messages));
        }
        RemoteCommand remoteCommand = new RemoteCommand(header, payLoad);
        FutureCommand futureCommand = new FutureCommand();
        futureCommand.setRequest(remoteCommand);
        WrapperFutureCommand wrappered = new WrapperFutureCommand(futureCommand,
                                        this.defaultMQProducer.getMaxRetryTime(),
                                        timeout, callback, messages[0].getTopic());
        return wrappered;
    }


    public void close() throws CraneClientException {
        this.clientInstance.unregisterProducer(id);
    }



    private void checkConfig() throws CraneClientException {
        if (StrUtil.isEmpty(this.defaultMQProducer.getTopic())) {
            throw new CraneClientException("Topic name cannot be null");
        }
        if (StrUtil.isEmpty(this.defaultMQProducer.getGroup())) {
            throw new CraneClientException("Group name cannot be null");
        }
        if (StrUtil.isEmpty(this.defaultMQProducer.getRegistryAddr())) {
            throw new CraneClientException("Brokder address cannot be null");
        }
        if (this.defaultMQProducer.getMaxRetryTime() < 0) {
            throw new CraneClientException("Max Retry Time cannot be negtive");
        }
    }
    public WrapperFutureCommand getWrapperFuture(String correlationID) {
        return this.clientInstance.getWrapperFuture(correlationID);
    }
    public void removeWrapperFuture(String correlationID) {
        this.clientInstance.removeWrapperFuture(correlationID);
    }
    public void asyncSend(WrapperFutureCommand wrapperFutureCommand) {
        this.clientInstance.sendMessageAsync(wrapperFutureCommand);
    }
    public RemoteAddress getAddress() {
        return address;
    }

    public void setAddress(RemoteAddress address) {
        this.address = address;
    }

    @Override
    public List<String> getBrokerAddress() {
        return null;
    }

    @Override
    public String fetechOrCreateTopic(int queueNumber) {
        return null;
    }
    public Set<String> getTopics() {
        return this.topicSet;
    }

    public String[] getRegisteryAddress() {
        return registryAddress.split(";");
    }
    public String getOriginRegisteryAddress() {
        return registryAddress;
    }
    public void setRegistryAddress(String registryAddress) {
        this.registryAddress = registryAddress;
    }

    public void setLoadBalanceStrategy(LoadBalanceStrategy loadBalanceStrategy) {
        this.loadBalanceStrategy = loadBalanceStrategy;
    }

    public DefaultMQProducer getDefaultMQProducer() {
        return defaultMQProducer;
    }

    public RegistryType getRegistryType() {
        return registryType;
    }

    public void setRegistryType(RegistryType registryType) {
        this.registryType = registryType;
    }
}
