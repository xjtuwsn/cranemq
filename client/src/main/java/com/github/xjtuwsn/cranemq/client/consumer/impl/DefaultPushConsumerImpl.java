package com.github.xjtuwsn.cranemq.client.consumer.impl;

import cn.hutool.core.collection.ConcurrentHashSet;
import cn.hutool.core.lang.Pair;
import com.github.xjtuwsn.cranemq.client.remote.WrapperFutureCommand;
import com.github.xjtuwsn.cranemq.client.consumer.DefaultPushConsumer;
import com.github.xjtuwsn.cranemq.client.consumer.PullResult;
import com.github.xjtuwsn.cranemq.client.consumer.listener.CommonMessageListener;
import com.github.xjtuwsn.cranemq.client.consumer.listener.MessageListener;
import com.github.xjtuwsn.cranemq.client.consumer.listener.OrderedMessageListener;
import com.github.xjtuwsn.cranemq.client.consumer.offset.BrokerOffsetManager;
import com.github.xjtuwsn.cranemq.client.consumer.offset.OffsetManager;
import com.github.xjtuwsn.cranemq.client.consumer.push.*;
import com.github.xjtuwsn.cranemq.client.consumer.rebalance.ConsistentHashAllocation;
import com.github.xjtuwsn.cranemq.client.consumer.rebalance.QueueAllocation;
import com.github.xjtuwsn.cranemq.client.hook.PullCallback;
import com.github.xjtuwsn.cranemq.client.remote.ClientFactory;
import com.github.xjtuwsn.cranemq.client.remote.ClientInstance;
import com.github.xjtuwsn.cranemq.common.command.FutureCommand;
import com.github.xjtuwsn.cranemq.common.command.Header;
import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.command.payloads.req.MQPullMessageRequest;
import com.github.xjtuwsn.cranemq.common.command.types.AcquireResultType;
import com.github.xjtuwsn.cranemq.common.command.types.RequestType;
import com.github.xjtuwsn.cranemq.common.command.types.RpcType;
import com.github.xjtuwsn.cranemq.common.constant.MQConstant;
import com.github.xjtuwsn.cranemq.common.consumer.MessageModel;
import com.github.xjtuwsn.cranemq.common.consumer.StartConsume;
import com.github.xjtuwsn.cranemq.common.consumer.SubscriptionInfo;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.github.xjtuwsn.cranemq.common.entity.ReadyMessage;
import com.github.xjtuwsn.cranemq.common.remote.RemoteHook;
import com.github.xjtuwsn.cranemq.common.remote.enums.RegistryType;
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
 * 默认的push消费者实现
 */
public class DefaultPushConsumerImpl {
    private static final Logger log = LoggerFactory.getLogger(DefaultPushConsumerImpl.class);
    private DefaultPushConsumer defaultPushConsumer;
    private String clientId;
    // topic: subscriptions  topic和其订阅关系
    private Map<String, SubscriptionInfo> topicTags = new ConcurrentHashMap<>();

    private Set<String> topicSet = new ConcurrentHashSet<>();
    private MessageListener messageListener;
    private String[] registerAddress;
    private RemoteHook hook;
    private ClientInstance clientInstance;
    private QueueAllocation queueAllocation;
    private ConsumeMessageService consumeMessageService;

    private OffsetManager offsetManager;

    private boolean isGray;
    
    private MessageQueueLock messageQueueLock;
    private RegistryType registryType = RegistryType.DEFAULT;

    public DefaultPushConsumerImpl(DefaultPushConsumer defaultPushConsumer, RemoteHook hook) {
        this.defaultPushConsumer = defaultPushConsumer;
        this.hook = hook;

        this.queueAllocation = new ConsistentHashAllocation();
        this.messageQueueLock = new MessageQueueLock();
    }
    public DefaultPushConsumerImpl(DefaultPushConsumer defaultPushConsumer, RemoteHook hook,
                                   String address, List<Pair<String, String>> topics, RegistryType registryType,
                                   boolean isGray, QueueAllocation queueAllocation) {
        this(defaultPushConsumer, hook);
        this.bindRegistry(address, registryType);
        this.subscribe(topics);
        this.isGray = isGray;
        this.queueAllocation = queueAllocation;

    }
    public void start() {
        String retryTopic = MQConstant.RETRY_PREFIX + this.defaultPushConsumer.getConsumerGroup();
        // 默认订阅重试队列
        this.subscribe(retryTopic, "*");
        this.clientId = TopicUtil.buildClientID("push_consumer") + defaultPushConsumer.getId();
        this.clientInstance = ClientFactory.newInstance().getOrCreate(clientId, hook);
        this.messageListener = defaultPushConsumer.getMessageListener();
        // 普通消息与顺序消息
        if (messageListener instanceof CommonMessageListener) {
            consumeMessageService = new CommonConsumeMessageService(messageListener, this);
        } else if (messageListener instanceof OrderedMessageListener) {
            consumeMessageService = new OrderedConsumeMessageService(messageListener, this);
        }
        this.consumeMessageService.start();
        this.clientInstance.registerPushConsumer(defaultPushConsumer.getConsumerGroup(), this);
        this.clientInstance.registerHook(hook);
        this.clientInstance.setRegistryType(registryType);
        this.clientInstance.start();
        if (defaultPushConsumer.getMessageModel() == MessageModel.CLUSTER) {
            this.offsetManager = new BrokerOffsetManager(this.clientInstance, this.defaultPushConsumer.getConsumerGroup());
        } else {
            this.offsetManager = this.clientInstance.getOffsetManager();
        }
        if (this.offsetManager != null) {
            this.offsetManager.start();
        }
        log.info("Default push consumer start successfully");

    }
    public void shutdown() {
        this.offsetManager.persistOffset();

        this.clientInstance.unregisterPushConsumer(defaultPushConsumer.getConsumerGroup());

    }

    /**
     * 发送拉的请求，从rebalance服务那里调用
     * @param request
     */
    public void pull(PullRequest request) {
        if (request == null) {
            log.warn("Pull Request cannot be null");
            return;
        }
        // 封装请求
        String group = request.getGroupName();
        MessageQueue queue = request.getMessageQueue();
        BrokerQueueSnapShot snapShot = request.getSnapShot();
        Header header = new Header(RequestType.PULL_MESSAGE, RpcType.ASYNC, TopicUtil.generateUniqueID());

        PayLoad payLoad = new MQPullMessageRequest(this.clientId, group, queue, request.getOffset(),
                offsetManager.readOffset(queue, group));
        RemoteCommand remoteCommand = new RemoteCommand(header, payLoad);
        FutureCommand futureCommand = new FutureCommand(remoteCommand);
        WrapperFutureCommand wrappered = new WrapperFutureCommand(futureCommand, queue.getTopic());
        // 拉取结果的回调
        PullCallback pullCallback = new PullCallback() {
            @Override
            public void onSuccess(PullResult pullResult) {
                if (pullResult == null) {
                    log.warn("Receive null pull result");
                    return;
                }
                if (snapShot.isExpired()) {
                    return;
                }
                AcquireResultType type = pullResult.getAcquireResultType();
                switch (type) {
                    // 拉取成功，更新位移，提交消费，并立刻拉取下一批
                    case DONE:
                        filterTags(pullResult, topicTags.get(queue.getTopic()));
                        long prevOffset = request.getOffset();
                        long nextOffset = pullResult.getNextOffset();
                        request.setOffset(nextOffset);
                        List<ReadyMessage> messages = pullResult.getMessages();
                        snapShot.putMessage(messages);
                        if (consumeMessageService != null) {

                            consumeMessageService.submit(queue, snapShot, messages);
                        }
                        // clientInstance.getPullMessageService().putRequestDelay(request, 200);
                        clientInstance.getPullMessageService().putRequestNow(request);
                        break;
                    case NO_MESSAGE:
                    case OFFSET_INVALID:
                    case ERROR:
                        // 失败的情况，也进行重新拉取
                        clientInstance.getPullMessageService().putRequestNow(request);
                    default:
                        break;
                }
            }

            @Override
            public void onException(Throwable cause) {

            }
        };
        wrappered.setPullCallback(pullCallback);
        this.clientInstance.sendMessageAsync(wrappered);
    }
    public void subscribe(List<Pair<String, String>> infos) {
        for (Pair<String, String> topic : infos) {
            this.subscribe(topic.getKey(), topic.getValue());
        }
    }
    public void subscribe(String topic, String tags) {
        String[] tag = tags.split(",");
        Set<String> tagSet = new HashSet<>(Arrays.asList(tag));
        SubscriptionInfo subscriptionInfo = topicTags.get(topic);
        if (subscriptionInfo == null) {
            subscriptionInfo = new SubscriptionInfo(topic, tagSet);
            this.topicTags.put(topic, subscriptionInfo);
        } else {
            subscriptionInfo.getTag().addAll(tagSet);
        }
        this.topicSet.add(topic);
    }
    public void bindRegistry(String address, RegistryType registryType) {
        this.registerAddress = address.split(";");
        this.registryType = registryType;
    }
    private void filterTags(PullResult pullResult, SubscriptionInfo info) {
        if (pullResult.getAcquireResultType() != AcquireResultType.DONE) {
            return;
        }
        List<ReadyMessage> messages = pullResult.getMessages();
        Set<String> tag = info.getTag();
        List<ReadyMessage> collect = messages.stream().filter(e -> {
            if (tag.contains("*")) {
                return true;
            }
            return tag.contains(e);
        }).collect(Collectors.toList());
        // log.info("Before filter array is {}", messages);
        // log.info("After filter array is {}", collect);

        pullResult.setMessages(collect);
    }

    public void markGray(boolean isGray) {
        this.isGray = isGray;
    }

    public String[] getRegisterAddress() {
        return registerAddress;
    }

    public Set<String> getTopicSet() {
        return topicSet;
    }

    public QueueAllocation getQueueAllocation() {
        return queueAllocation;
    }

    public void setQueueAllocation(QueueAllocation queueAllocation) {
        this.queueAllocation = queueAllocation;
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

    public OffsetManager getOffsetManager() {
        return offsetManager;
    }

    public DefaultPushConsumer getDefaultPushConsumer() {
        return defaultPushConsumer;
    }

    public MessageQueueLock getMessageQueueLock() {
        return messageQueueLock;
    }

    public ClientInstance getClientInstance() {
        return clientInstance;
    }

    public String clientIdWihGray() {
        return isGray ? clientId + MQConstant.GRAY_SUFFIX : clientId;
    }

    public boolean isGray() {
        return isGray;
    }

    public boolean needLock() {
        return this.getMessageModel() == MessageModel.CLUSTER && messageListener != null
                && (messageListener instanceof OrderedMessageListener);
    }
}
