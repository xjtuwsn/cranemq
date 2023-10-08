package com.github.xjtuwsn.cranemq.client.consumer.impl;

import cn.hutool.core.collection.ConcurrentHashSet;
import com.github.xjtuwsn.cranemq.client.consumer.DefaultPushConsumer;
import com.github.xjtuwsn.cranemq.client.consumer.PullResult;
import com.github.xjtuwsn.cranemq.client.consumer.listener.MessageListener;
import com.github.xjtuwsn.cranemq.client.consumer.push.BrokerQueueSnapShot;
import com.github.xjtuwsn.cranemq.client.consumer.push.PullRequest;
import com.github.xjtuwsn.cranemq.client.consumer.rebalance.AverageQueueAllocation;
import com.github.xjtuwsn.cranemq.client.consumer.rebalance.QueueAllocation;
import com.github.xjtuwsn.cranemq.client.remote.ClienFactory;
import com.github.xjtuwsn.cranemq.client.remote.ClientInstance;
import com.github.xjtuwsn.cranemq.common.command.Header;
import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.command.payloads.req.MQPullMessageRequest;
import com.github.xjtuwsn.cranemq.common.command.types.RequestType;
import com.github.xjtuwsn.cranemq.common.command.types.RpcType;
import com.github.xjtuwsn.cranemq.common.consumer.MessageModel;
import com.github.xjtuwsn.cranemq.common.consumer.StartConsume;
import com.github.xjtuwsn.cranemq.common.consumer.SubscriptionInfo;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.github.xjtuwsn.cranemq.common.remote.RemoteHook;
import com.github.xjtuwsn.cranemq.common.utils.TopicUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @project:cranemq
 * @file:DefaultPushConsumerImpl
 * @author:wsn
 * @create:2023/10/08-10:50
 */
public class DefaultPushConsumerImpl {
    private static final Logger log = LoggerFactory.getLogger(DefaultPushConsumerImpl.class);
    private DefaultPushConsumer defaultPushConsumer;
    private String clientId;

    private Map<String, SubscriptionInfo> topicTags = new ConcurrentHashMap<>();

    private Set<String> topicSet = new ConcurrentHashSet<>();
    private MessageListener messageListener;
    private String[] registeryAddress;
    private RemoteHook hook;
    private ClientInstance clientInstance;
    private QueueAllocation queueAllocation;

    public DefaultPushConsumerImpl(DefaultPushConsumer defaultPushConsumer, RemoteHook hook) {
        this.defaultPushConsumer = defaultPushConsumer;
        this.hook = hook;
        this.clientId = TopicUtil.buildClientID("push_consumer");
        this.clientInstance = ClienFactory.newInstance().getOrCreate(clientId, hook);
        this.queueAllocation = new AverageQueueAllocation();
    }
    public void start() {
        this.clientInstance.registerPushConsumer(defaultPushConsumer.getConsumerGroup(), this);
        this.clientInstance.registerHook(hook);
        this.clientInstance.start();

    }
    public void shutdown() {

    }
    // TODO 发送拉的请求，broker处理长轮询，这边设置回调处理
    public void pull(PullRequest request) {
        if (request == null) {
            log.warn("Pull Request cannot be null");
            return;
        }
        String group = request.getGroupName();
        MessageQueue queue = request.getMessageQueue();
        BrokerQueueSnapShot snapShot = request.getSnapShot();
        Header header = new Header(RequestType.PULL_MESSAGE, RpcType.ASYNC, TopicUtil.generateUniqueID());
        PayLoad payLoad = new MQPullMessageRequest(group, queue);
    }
    public void subscribe(String topic, String tags) {
        String[] tag = tags.split(",");
        Set<String> tagSet = new HashSet<>(Arrays.asList(tag));
        SubscriptionInfo subscriptionInfo = new SubscriptionInfo(topic, tagSet);
        this.topicTags.put(tags, subscriptionInfo);
        this.topicSet.add(topic);
    }
    public void bindRegistry(String address) {
        this.registeryAddress = address.split(";");
    }

    public String[] getRegisteryAddress() {
        return registeryAddress;
    }

    public Set<String> getTopicSet() {
        return topicSet;
    }

    public QueueAllocation getQueueAllocation() {
        return queueAllocation;
    }
    public MessageModel getMessageModel() {
        return defaultPushConsumer.getMessageModel();
    }
    public StartConsume getStartConsume() {
        return defaultPushConsumer.getStartConsume();
    }
    public Set<SubscriptionInfo> getSubscriptionInfos() {
        return new HashSet<>(this.topicTags.values());
    }
}
