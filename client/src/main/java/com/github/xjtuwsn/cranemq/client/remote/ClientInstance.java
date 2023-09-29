package com.github.xjtuwsn.cranemq.client.remote;

import com.github.xjtuwsn.cranemq.client.hook.InnerCallback;
import com.github.xjtuwsn.cranemq.client.hook.SendCallback;
import com.github.xjtuwsn.cranemq.client.producer.impl.DefaultMQProducerImpl;
import com.github.xjtuwsn.cranemq.client.producer.impl.WrapperFutureCommand;
import com.github.xjtuwsn.cranemq.client.producer.result.SendResult;
import com.github.xjtuwsn.cranemq.client.producer.result.SendResultType;
import com.github.xjtuwsn.cranemq.common.command.payloads.MQUpdateTopicResponse;
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
        this.remoteClent = new RemoteClent();
        this.remoteClent.start();
        this.clinetNumber = new AtomicInteger(0);
        this.registryAddress = "127.0.0.1:11111";
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
        this.retryService = Executors.newScheduledThreadPool(2, new ThreadFactory() {
            AtomicInteger count = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "ScheduledThreadPool no." + count.getAndIncrement());
            }
        });
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
        this.timerService.scheduleAtFixedRate(() -> {
            log.info("Fetech topic router info from registery");
            this.fetchRouteInfo();
        }, 30, 1000 * 30, TimeUnit.MILLISECONDS);
    }
    public void invoke(String topic, RemoteCommand command) throws CraneClientException {

        TopicRouteInfo routeInfo = this.topicTable.get(topic);
        if (routeInfo == null) {
            this.fetchRouteInfo();
            routeInfo = this.topicTable.get(topic);
        }
        String address = routeInfo.getBrokerData().get(0).getBrokerAddress();
        this.remoteClent.invoke(address, command);
    }
    public SendResult sendMessageSync(final WrapperFutureCommand wrappered, boolean isOneWay) {
        FutureCommand futureCommand = wrappered.getFutureCommand();
        if (this.hook != null) {
            this.hook.beforeMessage();
        }
        this.asyncSendThreadPool.execute(() -> {
            sendCore(wrappered, null);
        });
        if (isOneWay) return null;
        SendResult result = null;
        try {
            RemoteCommand response = futureCommand.get();
            log.info("Sync method get response: {}", response);
        } catch (InterruptedException | ExecutionException | CraneClientException e) {
            result = new SendResult(SendResultType.SERVER_ERROR, futureCommand.getRequest().getHeader().getCorrelationId());
            log.warn("Sync Request has retred for max time");
        }
        if (result == null) {

            result = new SendResult(SendResultType.SEDN_OK, futureCommand.getRequest().getHeader().getCorrelationId());
        }
        // TODO 返回SendResult根据结果设置 DONE
        return result;
    }
    public void sendMessateAsync(final WrapperFutureCommand wrappered) {
        this.asyncSendThreadPool.execute(() -> {
            sendCore(wrappered, wrappered.getCallback());
        });
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
        this.invoke(wrappered.getTopic(), remoteCommand);

    }

    public WrapperFutureCommand getWrapperFuture(String correlationID) {
        return requestTable.get(correlationID);
    }

    public void removeWrapperFuture(String correlationID) {
        requestTable.remove(correlationID);
    }

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
    private void getTopicInfoSync(String topic) {

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
                if (info == null) {
                    log.error("Cannot find route info {} from registery", topic);
                    return;
                }
                topicTable.put(topic, info);
            }
        });
        this.sendMessateAsync(wrappered);


    }
    public void shutdown() {
        this.remoteClent.shutdown();
    }


    public void registerHook(RemoteHook hook) {
        this.hook = hook;
    }
    public void registerProducer(DefaultMQProducerImpl defaultMQProducer) {
        int order = this.clinetNumber.getAndIncrement();
        this.producerRegister.put("produer-" + order, defaultMQProducer);
    }
}
