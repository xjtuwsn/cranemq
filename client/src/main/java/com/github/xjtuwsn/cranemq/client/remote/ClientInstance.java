package com.github.xjtuwsn.cranemq.client.remote;

import com.github.xjtuwsn.cranemq.client.hook.InnerCallback;
import com.github.xjtuwsn.cranemq.client.hook.SendCallback;
import com.github.xjtuwsn.cranemq.client.producer.balance.LoadBalanceStrategy;
import com.github.xjtuwsn.cranemq.client.producer.balance.RandomLoadBalance;
import com.github.xjtuwsn.cranemq.client.producer.impl.DefaultMQProducerImpl;
import com.github.xjtuwsn.cranemq.client.producer.impl.WrapperFutureCommand;
import com.github.xjtuwsn.cranemq.client.producer.result.SendResult;
import com.github.xjtuwsn.cranemq.client.producer.result.SendResultType;
import com.github.xjtuwsn.cranemq.common.command.payloads.MQUpdateTopicResponse;
import com.github.xjtuwsn.cranemq.common.command.types.ResponseType;
import com.github.xjtuwsn.cranemq.common.route.TopicRouteInfo;
import com.github.xjtuwsn.cranemq.common.command.FutureCommand;
import com.github.xjtuwsn.cranemq.common.command.Header;
import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.command.payloads.MQUpdateTopicRequest;
import com.github.xjtuwsn.cranemq.common.command.types.RequestType;
import com.github.xjtuwsn.cranemq.common.command.types.RpcType;
import com.github.xjtuwsn.cranemq.common.exception.CraneClientException;
import com.github.xjtuwsn.cranemq.common.utils.TopicUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.xjtuwsn.cranemq.client.hook.RemoteHook;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @project:cranemq
 * @file:RemoteClient
 * @author:wsn
 * @create:2023/09/27-14:59
 */
public class ClientInstance {

    private static final Logger log= LoggerFactory.getLogger(ClientInstance.class);
    private DefaultMQProducerImpl defaultMQProducer;
    private LoadBalanceStrategy loadBalanceStrategy = new RandomLoadBalance();

    private RemoteHook hook;
    private RemoteClent remoteClent;
    private ExecutorService asyncSendThreadPool;
    private String registryAddress;

    private ScheduledExecutorService retryService;
    private ScheduledExecutorService timerService;
    private int coreSize = 3;

    private int maxSize = 5;

    private ConcurrentHashMap<String, TopicRouteInfo> topicTable = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, DefaultMQProducerImpl> producerRegister = new ConcurrentHashMap<>();
    private AtomicInteger clinetNumber;
    private static volatile ConcurrentHashMap<String, WrapperFutureCommand> requestTable = new ConcurrentHashMap<>();



    public ClientInstance(DefaultMQProducerImpl impl) {
        this.defaultMQProducer = impl;

    }

    public void start() {
        this.remoteClent = new RemoteClent(this.defaultMQProducer);
        this.remoteClent.registerHook(hook);
        this.remoteClent.start();
        this.clinetNumber = new AtomicInteger(0);
        this.registryAddress = this.defaultMQProducer.getRegisteryAddress();
        // 异步发送消息的线程池
        this.asyncSendThreadPool = new ThreadPoolExecutor(coreSize,
                maxSize,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(8000),
                new ThreadFactory() {
                    AtomicInteger count = new AtomicInteger(0);
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "AsyncSendThreadPool no." + count.getAndIncrement());
                    }
                },
                new ThreadPoolExecutor.AbortPolicy());
        // 对于每一个消息，定时判断是否删除的线程池
        this.retryService = Executors.newScheduledThreadPool(2, new ThreadFactory() {
            AtomicInteger count = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "ScheduledThreadPool no." + count.getAndIncrement());
            }
        });
        // 定时向registry更新路由的线程池
        this.timerService = Executors.newScheduledThreadPool(4, new ThreadFactory() {
            AtomicInteger count = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "ScheduledThreadPool no." + count.getAndIncrement());
            }
        });
        this.startScheduleTast();
    }
    private void startScheduleTast() {
        // 每30s向注册中心更新路由
        this.timerService.scheduleAtFixedRate(() -> {
            log.info("Fetech topic router info from registery");
            this.fetchRouteInfo();
        }, 30, 1000 * 30, TimeUnit.MILLISECONDS);
        this.timerService.scheduleAtFixedRate(() -> {
            log.info("Clean expired remote broker");
            this.cleanExpired();
        }, 30, 1000 * 60, TimeUnit.MILLISECONDS);
    }

    public void invoke(String topic, WrapperFutureCommand wrappered) throws CraneClientException {
        RemoteCommand command = wrappered.getFutureCommand().getRequest();
        String address = "";
        if (wrappered.isToRegistery()) {
            address = this.registryAddress;
        } else {
            TopicRouteInfo routeInfo = this.topicTable.get(topic);
            if (routeInfo == null) {
                this.getTopicInfoSync(topic);
                routeInfo = this.topicTable.get(topic);
            }
            if (routeInfo == null) {
                throw new CraneClientException("Registery or network error");
            }
            address = routeInfo.getBrokerData().get(0).getBrokerAddress();
        }

        this.remoteClent.invoke(address, command);
    }
    public SendResult sendMessageSync(final WrapperFutureCommand wrappered, boolean isOneWay) {
        FutureCommand futureCommand = wrappered.getFutureCommand();
        if (this.hook != null && !wrappered.isToRegistery()) {
            this.hook.beforeMessage();
        }
        this.asyncSendThreadPool.execute(() -> {
            sendCore(wrappered, null);
        });
        if (isOneWay) return null;
        SendResult result = null;
        RemoteCommand response = null;
        try {
            response = futureCommand.get();
            log.info("Sync method get response: {}", response);
        } catch (InterruptedException | ExecutionException | CraneClientException e) {
            result = new SendResult(SendResultType.SERVER_ERROR, futureCommand.getRequest().getHeader().getCorrelationId());
            log.warn("Sync Request has retred for max time");
        }
        if (result == null) {
            result = this.buildSendResult(response, futureCommand.getRequest().getHeader().getCorrelationId());
        }
        // TODO 返回SendResult根据结果设置 DONE
        return result;
    }
    public void sendMessateAsync(final WrapperFutureCommand wrappered) {
        this.asyncSendThreadPool.execute(() -> {
            sendCore(wrappered, wrappered.getCallback());
        });
    }
    private SendResult buildSendResult(RemoteCommand reponse, String correlationID) {
        if (reponse == null) {
            log.error("Receive empty response, id is {}", correlationID);
            return new SendResult(SendResultType.SERVER_ERROR, correlationID);
        }
        SendResult result = new SendResult(SendResultType.SEDN_OK, correlationID);
        if (reponse.getHeader().getCommandType() == ResponseType.UPDATE_TOPIC_RESPONSE) {
            MQUpdateTopicResponse mqUpdateTopicResponse = (MQUpdateTopicResponse) reponse.getPayLoad();
            if (mqUpdateTopicResponse == null) {
                result.setResultType(SendResultType.SERVER_ERROR);
            } else {

                result.setTopic(mqUpdateTopicResponse.getTopic());
                result.setTopicRouteInfo(mqUpdateTopicResponse.getRouteInfo());
            }
        }
        return result;
    }
    private void sendCore(final WrapperFutureCommand wrappered, SendCallback callback) {
        RemoteCommand remoteCommand = wrappered.getFutureCommand().getRequest();
        RpcType rpcType = remoteCommand.getHeader().getRpcType();
        String correlationID = remoteCommand.getHeader().getCorrelationId();
        if (rpcType != RpcType.ONE_WAY) {
            requestTable.putIfAbsent(correlationID, wrappered);
            if (wrappered.getTimeout() > 0) {
                this.retryService.schedule(() -> {
                    WrapperFutureCommand newWrappered = requestTable.get(correlationID);
                    // 已经被删除了
                    if (newWrappered == null) {
                        log.info("{} has aready been deleted, wont do timeout", correlationID);
                        return;
                    }
                    // 被设置了完成标识但未删除
                    if (newWrappered.isDone()) {
                        log.info("{} has aready done, wont do timeout", correlationID);
                        requestTable.remove(correlationID);
                        return;
                    }
                    log.warn("Request {} has timeout", correlationID);
                    // 已达到最大重试次数
                    if (!newWrappered.isNeedRetry()) {
                        log.warn("Request {} has timeout for max retry time", correlationID);
                        newWrappered.cancel();
                        newWrappered.getCallback().onFailure(new TimeoutException("Timeout"));
                        requestTable.remove(correlationID);
                        throw new CraneClientException("Request timeout, has retry for max time");
                    }
                    // 又添加了新的任务，这个任务在延时期间收到了错误的响应，导致重试
                    if (newWrappered.isExpired()) {
                        log.info("Request has retried in response processor");
                        return;
                    }
                    // 重试
                    newWrappered.increaseRetryTime();
                    newWrappered.setStartTime(System.currentTimeMillis());
                    this.asyncSendThreadPool.execute(() -> {
                        log.info("Request {} do retry", correlationID);
                        sendCore(newWrappered, callback);
                    });

                }, wrappered.getTimeout(), TimeUnit.MILLISECONDS);
            }
        }
        this.invoke(wrappered.getTopic(), wrappered);

    }

    public WrapperFutureCommand getWrapperFuture(String correlationID) {
        return requestTable.get(correlationID);
    }

    public void removeWrapperFuture(String correlationID) {
        requestTable.remove(correlationID);
    }

    /**
     * 定期抓取注册中心数据，异步
     */
    private void fetchRouteInfo() {
        Set<String> topicList = new HashSet<>();
        for (Map.Entry<String, DefaultMQProducerImpl> entry : this.producerRegister.entrySet()) {
            DefaultMQProducerImpl impl = entry.getValue();
            if (impl != null) {
                topicList.addAll(impl.getTopics());
            }
        }
        for (String topic : topicList) {
            this.updateTopicInfo(topic);
        }
    }
    // TODO 检查发往注册中心更新路由的逻辑，启动一个简单注册中心验证
    /**
     * 缺少当前topic数据时，同步的从注册中心取得数据
     * @param topic
     */
    private void getTopicInfoSync(String topic) {
        Header header = new Header(RequestType.UPDATE_TOPIC_REQUEST,
                RpcType.SYNC, TopicUtil.generateUniqueID());
        PayLoad payLoad = new MQUpdateTopicRequest(topic);
        RemoteCommand remoteCommand = new RemoteCommand(header, payLoad);
        FutureCommand futureCommand = new FutureCommand();
        futureCommand.setRequest(remoteCommand);
        WrapperFutureCommand wrappered = new WrapperFutureCommand(futureCommand, topic, -1, null);
        wrappered.setToRegistery(true);
        SendResult result = this.sendMessageSync(wrappered, false);
        if (result.getResultType() == SendResultType.SERVER_ERROR || result.getTopicRouteInfo() == null) {
            log.error("Topic {} cannot find correct broker", topic);
        }
        TopicRouteInfo old = this.topicTable.get(topic);
        this.markExpiredBroker(result.getTopicRouteInfo(), old);
        this.topicTable.put(topic, result.getTopicRouteInfo());
    }
    private void markExpiredBroker(TopicRouteInfo newInfo, TopicRouteInfo oldInfo) {
        if (oldInfo == null) {
            return;
        }
        List<String> expired = oldInfo.getExpiredBrokerAddress(newInfo);
        this.remoteClent.markExpired(expired);
    }
    private void cleanExpired() {
        this.remoteClent.cleanExpired();
    }
    // TODO 更新topic信息，从注册中心，创建响应信息，设置回调，更新结果
    private void updateTopicInfo(String topic) {
        Header header = new Header(RequestType.UPDATE_TOPIC_REQUEST,
                RpcType.ASYNC, TopicUtil.generateUniqueID());
        PayLoad payLoad = new MQUpdateTopicRequest(topic);
        RemoteCommand remoteCommand = new RemoteCommand(header, payLoad);
        FutureCommand futureCommand = new FutureCommand();
        futureCommand.setRequest(remoteCommand);
        WrapperFutureCommand wrappered = new WrapperFutureCommand(futureCommand, topic, -1, new InnerCallback() {
            @Override
            public void onResponse(RemoteCommand remoteCommand) {
                MQUpdateTopicResponse response = (MQUpdateTopicResponse) remoteCommand.getPayLoad();
                TopicRouteInfo info = response.getRouteInfo();
                TopicRouteInfo old = topicTable.get(topic);
                markExpiredBroker(response.getRouteInfo(), old);
                if (info == null) {
                    log.error("Cannot find route info {} from registery", topic);
                    return;
                }
                topicTable.put(topic, info);
            }
        });
        wrappered.setToRegistery(true);
        this.sendMessateAsync(wrappered);
    }
    public void shutdown() {
        this.remoteClent.shutdown();
        if (this.asyncSendThreadPool != null) {
            this.asyncSendThreadPool.shutdown();
        }
        if (this.retryService != null) {
            this.retryService.shutdown();
        }
        if (this.timerService != null) {
            this.timerService.shutdown();
        }

    }


    public void registerHook(RemoteHook hook) {
        this.hook = hook;
    }
    public void registerProducer(DefaultMQProducerImpl defaultMQProducer) {
        int order = this.clinetNumber.getAndIncrement();
        this.producerRegister.put("produer-" + order, defaultMQProducer);
    }

    public void setLoadBalanceStrategy(LoadBalanceStrategy loadBalanceStrategy) {
        this.loadBalanceStrategy = loadBalanceStrategy;
    }
}
