package com.github.xjtuwsn.cranemq.client.consumer.impl;

import cn.hutool.core.collection.ConcurrentHashSet;
import com.github.xjtuwsn.cranemq.client.WrapperFutureCommand;
import com.github.xjtuwsn.cranemq.client.consumer.DefaultPushConsumer;
import com.github.xjtuwsn.cranemq.client.consumer.PullResult;
import com.github.xjtuwsn.cranemq.client.consumer.listener.MessageListener;
import com.github.xjtuwsn.cranemq.client.consumer.push.BrokerQueueSnapShot;
import com.github.xjtuwsn.cranemq.client.consumer.push.CommonConsumeMessageService;
import com.github.xjtuwsn.cranemq.client.consumer.push.ConsumeMessageService;
import com.github.xjtuwsn.cranemq.client.consumer.push.PullRequest;
import com.github.xjtuwsn.cranemq.client.consumer.rebalance.AverageQueueAllocation;
import com.github.xjtuwsn.cranemq.client.consumer.rebalance.QueueAllocation;
import com.github.xjtuwsn.cranemq.client.hook.PullCallback;
import com.github.xjtuwsn.cranemq.client.remote.ClienFactory;
import com.github.xjtuwsn.cranemq.client.remote.ClientInstance;
import com.github.xjtuwsn.cranemq.common.command.FutureCommand;
import com.github.xjtuwsn.cranemq.common.command.Header;
import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.command.payloads.req.MQPullMessageRequest;
import com.github.xjtuwsn.cranemq.common.command.types.AcquireResultType;
import com.github.xjtuwsn.cranemq.common.command.types.RequestType;
import com.github.xjtuwsn.cranemq.common.command.types.RpcType;
import com.github.xjtuwsn.cranemq.common.consumer.MessageModel;
import com.github.xjtuwsn.cranemq.common.consumer.StartConsume;
import com.github.xjtuwsn.cranemq.common.consumer.SubscriptionInfo;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.github.xjtuwsn.cranemq.common.entity.ReadyMessage;
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
    private ConsumeMessageService commonMessageService;

    public DefaultPushConsumerImpl(DefaultPushConsumer defaultPushConsumer, RemoteHook hook) {
        this.defaultPushConsumer = defaultPushConsumer;
        this.hook = hook;
        this.clientId = TopicUtil.buildClientID("push_consumer");
        this.clientInstance = ClienFactory.newInstance().getOrCreate(clientId, hook);
        this.queueAllocation = new AverageQueueAllocation();
        if (messageListener instanceof  CommonConsumeMessageService) {
            commonMessageService = new CommonConsumeMessageService(messageListener);
        }
    }
    public void start() {
        if (commonMessageService != null) {
            commonMessageService.start();
        }
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
        RemoteCommand remoteCommand = new RemoteCommand(header, payLoad);
        FutureCommand futureCommand = new FutureCommand(remoteCommand);
        WrapperFutureCommand wrappered = new WrapperFutureCommand(futureCommand, queue.getTopic());
        PullCallback pullCallback = new PullCallback() {
            @Override
            public void onSuccess(PullResult pullResult) {
                if (pullResult == null) {
                    log.warn("Receive null pull result");
                    return;
                }
                filterTags(pullResult, topicTags.get(queue.getTopic()));
                AcquireResultType type = pullResult.getAcquireResultType();
                switch (type) {
                    case DONE:
                        List<ReadyMessage> messages = pullResult.getMessages();
                        snapShot.putMessage(messages);
                        if (commonMessageService != null) {
                            commonMessageService.submit(queue, snapShot, messages);
                        }
                        clientInstance.getPullMessageService().putRequestDelay(request, 300);
                        break;
                    case NO_MESSAGE:
                    case OFFSET_INVALID:
                    case ERROR:
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
    private void filterTags(PullResult pullResult, SubscriptionInfo info) {
        if (pullResult.getAcquireResultType() != AcquireResultType.DONE) {
            return;
        }
        List<ReadyMessage> messages = pullResult.getMessages();
        Set<String> tag = info.getTag();
        List<ReadyMessage> collect = messages.stream().filter(tag::contains).collect(Collectors.toList());
        log.info("Before filter array is {}", messages);
        log.info("After filter array is {}", collect);

        pullResult.setMessages(collect);
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
