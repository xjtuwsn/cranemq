package com.github.xjtuwsn.cranemq.client.producer.impl;

import cn.hutool.core.collection.ConcurrentHashSet;
import cn.hutool.core.util.StrUtil;
import com.github.xjtuwsn.cranemq.client.hook.SendCallback;
import com.github.xjtuwsn.cranemq.client.producer.result.SendResult;
import com.github.xjtuwsn.cranemq.client.producer.result.SendResultType;
import com.github.xjtuwsn.cranemq.common.command.*;
import com.github.xjtuwsn.cranemq.common.command.payloads.MQBachProduceRequest;
import com.github.xjtuwsn.cranemq.common.utils.TopicUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.xjtuwsn.cranemq.client.hook.RemoteHook;
import com.github.xjtuwsn.cranemq.client.producer.DefaultMQProducer;
import com.github.xjtuwsn.cranemq.client.producer.MQProducerInner;
import com.github.xjtuwsn.cranemq.client.remote.ClienFactory;
import com.github.xjtuwsn.cranemq.client.remote.ClientInstance;
import com.github.xjtuwsn.cranemq.common.command.payloads.MQProduceRequest;
import com.github.xjtuwsn.cranemq.common.command.types.RequestType;
import com.github.xjtuwsn.cranemq.common.command.types.RpcType;
import com.github.xjtuwsn.cranemq.common.entity.Message;
import com.github.xjtuwsn.cranemq.common.exception.CraneClientException;
import com.github.xjtuwsn.cranemq.common.net.RemoteAddress;
import com.github.xjtuwsn.cranemq.common.utils.NetworkUtil;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @project:cranemq
 * @file:DefaultMQProducerImpl
 * @author:wsn
 * @create:2023/09/27-14:38
 */
public class DefaultMQProducerImpl implements MQProducerInner {

    private static final Logger log = LoggerFactory.getLogger(DefaultMQProducer.class);

    private DefaultMQProducer defaultMQProducer;

    private RemoteHook hook;



    private ClientInstance clientInstance;
    private RemoteAddress address;
    private String clientID;
    private ConcurrentHashSet<String> topicSet = new ConcurrentHashSet<>();
    /**
     * 0: created
     * 1: started
     * 2: failed
     */
    private volatile AtomicInteger state;


    public DefaultMQProducerImpl(DefaultMQProducer defaultMQProducer, RemoteHook hook) {
        this.defaultMQProducer = defaultMQProducer;
        this.hook = hook;

        state = new AtomicInteger(0);
    }

    public void start() throws CraneClientException {
        if (this.state.get() != 0) {
            log.error("DefaultMQProducerImpl has already been started or faied!");
            throw new CraneClientException("DefaultMQProducerImpl has already been started or faied!");
        }
        this.checkConfig();
        this.address = this.defaultMQProducer.getBrokerAddress();
        this.clientID = buildClientID();
        this.clientInstance = ClienFactory.newInstance().getOrCreate(this.clientID, this);
        this.clientInstance.registerHook(hook);
//        this.remoteClient.start();


    }

    public SendResult sendSync(long timeout, boolean isOneWay, Message... messages) throws CraneClientException {
        if (messages == null || messages.length == 0) {
            throw new CraneClientException("Message cannot be empty!");
        }
        WrapperFutureCommand wrappered = buildRequest(RpcType.SYNC, null, timeout, messages);
        String correlationID = wrappered.getFutureCommand().getRequest().getHeader().getCorrelationId();
        if (isOneWay) {
            wrappered.getFutureCommand().getRequest().getHeader().setRpcType(RpcType.ONE_WAY);
        }
        if (wrappered == null) {
            throw new CraneClientException("Create Request error!");
        }
        FutureCommand futureCommand = wrappered.getFutureCommand();
        if (this.hook != null) {
            this.hook.beforeMessage();
        }
        return this.clientInstance.sendMessageSync(wrappered, isOneWay);
    }

    public void sendAsync(SendCallback callback, long timeout, Message... messages) {
        if (messages == null || messages.length == 0) {
            throw new CraneClientException("Message cannot be empty!");
        }
        WrapperFutureCommand remoteCommand = buildRequest(RpcType.ASYNC, callback, timeout, messages);
        if (remoteCommand == null) {
            throw new CraneClientException("Create Request error!");
        }
        if (this.hook != null) {
            this.hook.beforeMessage();
        }
        this.clientInstance.sendMessateAsync(remoteCommand);
    }


    /**
     * TODO 未添加响应消息处理，超时错误Failure设置
     * @param rpcType
     * @param callback
     * @param timeout
     * @param messages
     * @return
     */
    private WrapperFutureCommand buildRequest(RpcType rpcType, SendCallback callback, long timeout, Message... messages) {
        String topic = messages[0].getTopic();
        if (StrUtil.isEmpty(topic)) {
            throw new CraneClientException("Topic cannot be null");
        }
        if (!TopicUtil.checkTopic(topic)) {
            throw new CraneClientException("Topic name is invalid!");
        }
        this.topicSet.add(topic);
        String correlationID = TopicUtil.generateUniqueID();
        Header header = new Header(RequestType.MESSAGE_PRODUCE_REQUEST, RpcType.SYNC, correlationID);
        PayLoad payLoad = null;
        if (messages.length == 1) {
            payLoad = new MQProduceRequest(messages[0]);
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
        this.clientInstance.shutdown();
    }

    private String buildClientID() {
        String ip = NetworkUtil.getLocalAddress();
        StringBuilder id = new StringBuilder();
        id.append(ip).append("@").append(this.defaultMQProducer.getTopic())
                .append("@").append(this.defaultMQProducer.getGroup());
        return id.toString();
    }

    private void checkConfig() throws CraneClientException {
        if (StrUtil.isEmpty(this.defaultMQProducer.getTopic())) {
            throw new CraneClientException("Topic name cannot be null");
        }
        if (StrUtil.isEmpty(this.defaultMQProducer.getGroup())) {
            throw new CraneClientException("Group name cannot be null");
        }
        if (StrUtil.isEmpty(this.defaultMQProducer.getBrokerAddress().getAddress())) {
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
        this.clientInstance.sendMessateAsync(wrapperFutureCommand);
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
}
