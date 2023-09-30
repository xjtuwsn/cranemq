package com.github.xjtuwsn.cranemq.client.remote;

import cn.hutool.core.util.StrUtil;
import com.github.xjtuwsn.cranemq.client.hook.InnerCallback;
import com.github.xjtuwsn.cranemq.client.hook.SendCallback;
import com.github.xjtuwsn.cranemq.client.producer.balance.LoadBalanceStrategy;
import com.github.xjtuwsn.cranemq.client.producer.balance.RandomStrategy;
import com.github.xjtuwsn.cranemq.client.producer.impl.DefaultMQProducerImpl;
import com.github.xjtuwsn.cranemq.client.producer.impl.WrapperFutureCommand;
import com.github.xjtuwsn.cranemq.client.producer.result.SendResult;
import com.github.xjtuwsn.cranemq.client.producer.result.SendResultType;
import com.github.xjtuwsn.cranemq.common.command.payloads.MQCreateTopicRequest;
import com.github.xjtuwsn.cranemq.common.command.payloads.MQCreateTopicResponse;
import com.github.xjtuwsn.cranemq.common.command.payloads.MQUpdateTopicResponse;
import com.github.xjtuwsn.cranemq.common.command.types.ResponseCode;
import com.github.xjtuwsn.cranemq.common.command.types.ResponseType;
import com.github.xjtuwsn.cranemq.common.constant.MQConstant;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.github.xjtuwsn.cranemq.common.route.BrokerData;
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
    private LoadBalanceStrategy loadBalanceStrategy = new RandomStrategy();

    private RemoteHook hook;
    private RemoteClent remoteClent;
    private ExecutorService asyncSendThreadPool;
    private ExecutorService parallelCreateService;
    private String registryAddress;

    private ScheduledExecutorService retryService;
    private ScheduledExecutorService timerService;
    private int coreSize = 10;

    private int maxSize = 22;

    private volatile ConcurrentHashMap<String, TopicRouteInfo> topicTable = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, HashMap<Integer, String>> brokerAddressTable = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, DefaultMQProducerImpl> producerRegister = new ConcurrentHashMap<>();
    private AtomicInteger clinetNumber;
    private static volatile ConcurrentHashMap<String, WrapperFutureCommand> requestTable = new ConcurrentHashMap<>();
    private volatile Semaphore updateSemphore = new Semaphore(2);



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
        this.parallelCreateService = new ThreadPoolExecutor(coreSize,
                maxSize,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(1000),
                new ThreadFactory() {
                    AtomicInteger count = new AtomicInteger(0);
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "ParallelCreateService no." + count.getAndIncrement());
                    }
                },
                new ThreadPoolExecutor.AbortPolicy());
        // 对于每一个消息，定时判断是否删除的线程池
        this.retryService = Executors.newScheduledThreadPool(3, new ThreadFactory() {
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
        }, 300, 1000 * 30, TimeUnit.MILLISECONDS);
        this.timerService.scheduleAtFixedRate(() -> {
            log.info("Clean expired remote broker");
            this.cleanExpired();
        }, 300, 1000 * 60, TimeUnit.MILLISECONDS);
    }

    /**
     * 发送消息前最后一步，根据路由查找发送地址
     * @param topic
     * @param wrappered
     * @throws CraneClientException
     */
    public void invoke(String topic, WrapperFutureCommand wrappered) throws CraneClientException {
        RemoteCommand command = wrappered.getFutureCommand().getRequest();
        String address = "";
        // 查找路由的信息，地址就是注册中心地址

        if (wrappered.isToRegistery()) {
            address = this.registryAddress;
        } else {
            address = this.selectProducedQueueAndChangeHeader(wrappered, topic);
//            try {
//                TopicRouteInfo routeInfo = this.topicTable.get(topic);
//                // 不包含路由，去注册中心找
//                if (routeInfo == null) {
//                    this.updateSemphore.acquire();
//                    if (!this.topicTable.containsKey(topic)) {
//                        this.getTopicInfoSync(topic);
//                    }
//                    this.updateSemphore.release();
//                    routeInfo = this.topicTable.get(topic);
//                }
//                // 如果注册中心也没有，先查找默认路由，根据默认路由信息，向集群中所有节点创建topic
//                if (routeInfo == null) {
//                    String defaultTopic = MQConstant.DEFAULT_TOPIC_NAME;
//                    TopicRouteInfo defaultInfo = this.topicTable.get(defaultTopic);
//                    if (defaultInfo == null) {
//                        this.updateSemphore.acquire();
//                        if (!this.topicTable.containsKey(topic)) {
//                            this.getTopicInfoSync(defaultTopic);
//                        }
//                        this.updateSemphore.release();
//                        defaultInfo = this.topicTable.get(defaultTopic);
//                    }
//                    if (!this.topicTable.containsKey(topic)) {
//                        this.createTopicInCluster(defaultInfo, topic);
//                    }
//                }
//                // 根据路由选择一个队列地址
//                address = this.selectProducedQueueAndChangeHeader(wrappered, topic);
//            } catch (InterruptedException e) {
//                throw new CraneClientException("Semaphore has error");
//            }

        }

        this.remoteClent.invoke(address, command);
    }

    /**
     * 向默认路由包含的集群节点创建当前topic信息
     * @param topicRouteInfo
     */
    // TODO 当请求集中时，创建topic的请求可能会发送多次，需要broker端做处理
    private void createTopicInCluster(TopicRouteInfo topicRouteInfo, String createTopic) {
        if (topicRouteInfo == null) {
            throw new CraneClientException("Default route info is null!");
        }
        long start = System.nanoTime();
        String defaultTopic = topicRouteInfo.getTopic();
        List<String> brokers = topicRouteInfo.getBrokerAddresses();
        CopyOnWriteArrayList<SendResult> futures = new CopyOnWriteArrayList<>();
        // 等待所有broker创建完成
        CountDownLatch latch = new CountDownLatch(brokers.size());

        for (String addr : brokers) {
            Header header = new Header(RequestType.CREATE_TOPIC_REQUEST, RpcType.SYNC, TopicUtil.generateUniqueID());
            PayLoad payLoad = new MQCreateTopicRequest(createTopic);
            RemoteCommand remoteCommand = new RemoteCommand(header, payLoad);
            FutureCommand futureCommand = new FutureCommand(remoteCommand);
            WrapperFutureCommand wrappered = new WrapperFutureCommand(futureCommand,
                    defaultTopic, -1, null);
            // 多线程同时创建
            this.parallelCreateService.execute(() -> {
                log.info("Create topic on broker address: {}", addr);
                SendResult sendResult = this.sendMessageSync(wrappered, false);
                futures.add(sendResult);
                latch.countDown();
            });
        }
        try {
            latch.await();
            TopicRouteInfo newInfo = new TopicRouteInfo(createTopic);
            for (SendResult result : futures) {
                if (result.getResultType() == SendResultType.SEDN_OK) {
                    newInfo.compact(result.getTopicRouteInfo());
                }
            }
            this.topicTable.put(createTopic, newInfo);
            long end = System.nanoTime();
            double costMs = (end - start) / 1e6;
            log.info("Create topic {} cost {} ms", createTopic, costMs);
        } catch (InterruptedException e) {
            log.error("Waiting create thread has been interrupt!");
        }

    }
    // TODO 根据负载均衡选出队列，和指定地址，发送消息
    // TODO 更改路由表和相关逻辑
    private String selectProducedQueueAndChangeHeader(WrapperFutureCommand wrappered, String topic) {
        MessageQueue queue = this.loadBalanceStrategy.getNextQueue(topic, this.topicTable.get(topic));
        if (queue == null) {
            throw new CraneClientException("Queue select error");
        }
        String address = this.brokerAddressTable.get(queue.getBrokerName()).get(MQConstant.MASTER_ID);
        if (StrUtil.isEmpty(address)) {
            log.error("Cannot find address when getting {} broker", topic);
        }
        wrappered.getFutureCommand().getRequest().getHeader().setWriteQueue(queue);
        return address;
    }

    public SendResult sendMessageSync(final WrapperFutureCommand wrappered, boolean isOneWay) {
        FutureCommand futureCommand = wrappered.getFutureCommand();
        if (this.hook != null && !wrappered.isToRegistery()) {
            this.hook.beforeMessage();
        }
//        this.asyncSendThreadPool.execute(() -> {
//            sendCore(wrappered, null);
//        });
        this.sendAdaptor(wrappered, null);
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
    public void sendMessageAsync(final WrapperFutureCommand wrappered) {
//        this.asyncSendThreadPool.execute(() -> {
//            sendCore(wrappered, wrappered.getCallback());
//        });
        this.sendAdaptor(wrappered, wrappered.getCallback());
    }
    private SendResult buildSendResult(RemoteCommand response, String correlationID) {
        if (response == null) {
            log.error("Receive empty response, id is {}", correlationID);
            return new SendResult(SendResultType.SERVER_ERROR, correlationID);
        }
        SendResult result = new SendResult(SendResultType.SEDN_OK, correlationID);
        if (response.getHeader().getCommandType() == ResponseType.UPDATE_TOPIC_RESPONSE) {
            MQUpdateTopicResponse mqUpdateTopicResponse = (MQUpdateTopicResponse) response.getPayLoad();
            if (mqUpdateTopicResponse == null) {
                result.setResultType(SendResultType.SERVER_ERROR);
            } else {

                result.setTopic(mqUpdateTopicResponse.getTopic());
                result.setTopicRouteInfo(mqUpdateTopicResponse.getRouteInfo());
            }
        } else if (response.getHeader().getCommandType() == ResponseType.CREATE_TOPIC_RESPONSE) {
            MQCreateTopicResponse mqCreateTopicResponse = (MQCreateTopicResponse) response.getPayLoad();
            if (mqCreateTopicResponse == null || response.getHeader().getStatus() != ResponseCode.SUCCESS) {
                result.setResultType(SendResultType.SERVER_ERROR);
            } else {
                result.setTopicRouteInfo(mqCreateTopicResponse.getSingleBrokerInfo());
            }
        }
        return result;
    }
    private void sendAdaptor(final WrapperFutureCommand wrappered, SendCallback callback) {
        String topic = wrappered.getTopic();
        RemoteCommand command = wrappered.getFutureCommand().getRequest();
        String address = "";
        // 查找路由的信息，地址就是注册中心地址
        if (wrappered.isToRegistery()) {
            address = this.registryAddress;
        } else {
            try {
                TopicRouteInfo routeInfo = this.topicTable.get(topic);
                // 不包含路由，去注册中心找
                if (routeInfo == null) {
                    this.updateSemphore.acquire();
                    if (!this.topicTable.containsKey(topic)) {
                        this.getTopicInfoSync(topic);
                    }
                    this.updateSemphore.release();
                    routeInfo = this.topicTable.get(topic);
                }
                // 如果注册中心也没有，先查找默认路由，根据默认路由信息，向集群中所有节点创建topic
                if (routeInfo == null) {
                    String defaultTopic = MQConstant.DEFAULT_TOPIC_NAME;
                    TopicRouteInfo defaultInfo = this.topicTable.get(defaultTopic);
                    if (defaultInfo == null) {
                        this.updateSemphore.acquire();
                        if (!this.topicTable.containsKey(topic)) {
                            this.getTopicInfoSync(defaultTopic);
                        }
                        this.updateSemphore.release();
                        defaultInfo = this.topicTable.get(defaultTopic);
                    }
                    if (!this.topicTable.containsKey(topic)) {
                        this.createTopicInCluster(defaultInfo, topic);
                    }
                }
                // 根据路由选择一个队列地址
                // address = this.selectProducedQueueAndChangeHeader(wrappered, topic);
            } catch (InterruptedException e) {
                throw new CraneClientException("Semaphore has error");
            }

        }

        this.asyncSendThreadPool.execute(() -> {
            this.sendCore(wrappered, callback);
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
            return;
        }
        TopicRouteInfo old = this.topicTable.get(topic);
        this.markExpiredBroker(result.getTopicRouteInfo(), old);
        this.topicTable.put(topic, result.getTopicRouteInfo());
        for (BrokerData brokerData : result.getTopicRouteInfo().getBrokerData()) {
            brokerAddressTable.put(brokerData.getBrokerName(), brokerData.getBrokerAddressMap());
        }
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
                for (BrokerData brokerData : info.getBrokerData()) {
                    brokerAddressTable.put(brokerData.getBrokerName(), brokerData.getBrokerAddressMap());
                }
            }
        });
        wrappered.setToRegistery(true);
        this.sendMessageAsync(wrappered);
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
