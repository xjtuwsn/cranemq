package com.github.xjtuwsn.cranemq.client.consumer.impl;

import cn.hutool.core.collection.ConcurrentHashSet;
import cn.hutool.core.util.StrUtil;
import com.github.xjtuwsn.cranemq.client.consumer.DefaultPullConsumer;
import com.github.xjtuwsn.cranemq.client.consumer.PullResult;
import com.github.xjtuwsn.cranemq.client.remote.WrapperFutureCommand;
import com.github.xjtuwsn.cranemq.client.remote.ClientFactory;
import com.github.xjtuwsn.cranemq.client.remote.ClientInstance;
import com.github.xjtuwsn.cranemq.common.command.FutureCommand;
import com.github.xjtuwsn.cranemq.common.command.Header;
import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.command.payloads.req.MQSimplePullRequest;
import com.github.xjtuwsn.cranemq.common.command.types.RequestType;
import com.github.xjtuwsn.cranemq.common.command.types.RpcType;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.github.xjtuwsn.cranemq.common.exception.CraneClientException;
import com.github.xjtuwsn.cranemq.common.remote.RemoteHook;
import com.github.xjtuwsn.cranemq.common.remote.enums.RegistryType;
import com.github.xjtuwsn.cranemq.common.utils.TopicUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @project:cranemq
 * @file:DefaultPullConsumerImpl
 * @author:wsn
 * @create:2023/10/07-10:44
 * 拉取消息的消费者实现
 */
public class DefaultPullConsumerImpl {
    private static final Logger log = LoggerFactory.getLogger(DefaultPullConsumerImpl.class);
    private DefaultPullConsumer defaultPullConsumer;

    private RemoteHook hook;

    private String[] registryAddress;
    private String clientId;
    private String id;

    private ClientInstance clientInstance;
    private RegistryType registryType;

    private Map<String, String> topicTag = new ConcurrentHashMap<>();
    private Set<String> topicSet = new ConcurrentHashSet<>();



    public DefaultPullConsumerImpl(DefaultPullConsumer defaultPullConsumer, RemoteHook hook) {
        this.defaultPullConsumer = defaultPullConsumer;
        this.hook = hook;
        this.clientId = TopicUtil.buildClientID("pull_consumer");
        this.clientInstance = ClientFactory.newInstance().getOrCreate(this.clientId, this.hook);

    }
    public void subscribe(String topic, String tags) {
        this.topicSet.add(topic);
        this.topicTag.put(topic, tags);
    }

    /**
     * 根据给定的长度拉取消息
     * @param messageQueue
     * @param offset
     * @param len
     * @return
     * @throws CraneClientException
     */
    public PullResult pull(MessageQueue messageQueue, long offset, int len) throws CraneClientException {
        if (messageQueue == null || offset < 0 || len <= 0) {
            throw new CraneClientException("Illegal parameters!");
        }
        String id = TopicUtil.generateUniqueID();
        Header header = new Header(RequestType.SIMPLE_PULL_MESSAGE_REQUEST, RpcType.SYNC, id);
        PayLoad payLoad = new MQSimplePullRequest(messageQueue, offset,
                Math.min(len, defaultPullConsumer.getMaxPullLength()));
        RemoteCommand remoteCommand = new RemoteCommand(header, payLoad);
        FutureCommand futureCommand = new FutureCommand(remoteCommand);
        WrapperFutureCommand wrappered = new WrapperFutureCommand(futureCommand, defaultPullConsumer.getMaxRetryTime(),
                defaultPullConsumer.getMaxTimeoutMills(), null, messageQueue.getTopic());
        wrappered.setQueuePicked(messageQueue);
        return this.clientInstance.sendPullSync(wrappered);
    }
    public void bindRegistry(String address, RegistryType registryType) {
        if (StrUtil.isEmpty(address) || registryType == null) {
            throw new CraneClientException("Registry address cannot be empty");
        }
        this.registryType = registryType;
        this.registryAddress = address.split(";");
    }
    public List<MessageQueue> listQueues() {
        return this.clientInstance.listQueues(this.topicSet);
    }
    public List<MessageQueue> listQueues(String topic) {
        Set<String> set = new HashSet<>();
        set.add(topic);
        return this.clientInstance.listQueues(set);
    }
    public void start() {
        this.checkConfig();
        this.clientInstance.registerHook(this.hook);
        this.clientInstance.setRegistryType(registryType);
        id = this.clientInstance.registerPullConsumer(this);
        this.clientInstance.start();
    }
    public void shutdown() {
        this.clientInstance.unregisterPullConsumer(id);
    }

    private void checkConfig() throws CraneClientException {
        if (StrUtil.isEmpty(this.defaultPullConsumer.getConsumerGroup())) {
            throw new CraneClientException("Consumer group can not be empty");
        }
        if (this.defaultPullConsumer.getMessageModel() == null) {
            throw new CraneClientException("Message consume model can not be null");
        }
    }

    public Set<String> getTopicSet() {
        return topicSet;
    }

    public String[] getRegistryAddress() {
        return registryAddress;
    }
}
