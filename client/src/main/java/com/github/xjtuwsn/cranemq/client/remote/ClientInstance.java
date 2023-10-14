package com.github.xjtuwsn.cranemq.client.remote;

import cn.hutool.core.util.StrUtil;
import com.github.xjtuwsn.cranemq.client.consumer.PullResult;
import com.github.xjtuwsn.cranemq.client.consumer.RebalanceService;
import com.github.xjtuwsn.cranemq.client.consumer.impl.DefaultPullConsumerImpl;
import com.github.xjtuwsn.cranemq.client.consumer.impl.DefaultPushConsumerImpl;
import com.github.xjtuwsn.cranemq.client.consumer.offset.LocalOffsetManager;
import com.github.xjtuwsn.cranemq.client.consumer.offset.OffsetManager;
import com.github.xjtuwsn.cranemq.client.consumer.push.PullMessageService;
import com.github.xjtuwsn.cranemq.client.hook.InnerCallback;
import com.github.xjtuwsn.cranemq.client.hook.SendCallback;
import com.github.xjtuwsn.cranemq.client.processor.CommonProcessor;
import com.github.xjtuwsn.cranemq.client.processor.ConsumerProcessor;
import com.github.xjtuwsn.cranemq.client.processor.PruducerProcessor;
import com.github.xjtuwsn.cranemq.client.producer.MQSelector;
import com.github.xjtuwsn.cranemq.client.producer.balance.LoadBalanceStrategy;
import com.github.xjtuwsn.cranemq.client.producer.balance.RandomStrategy;
import com.github.xjtuwsn.cranemq.client.producer.impl.DefaultMQProducerImpl;
import com.github.xjtuwsn.cranemq.client.WrapperFutureCommand;
import com.github.xjtuwsn.cranemq.client.producer.result.SendResult;
import com.github.xjtuwsn.cranemq.client.producer.result.SendResultType;
import com.github.xjtuwsn.cranemq.common.command.payloads.req.*;
import com.github.xjtuwsn.cranemq.common.command.payloads.resp.MQCreateTopicResponse;
import com.github.xjtuwsn.cranemq.common.command.payloads.resp.MQLockRespnse;
import com.github.xjtuwsn.cranemq.common.command.payloads.resp.MQSimplePullResponse;
import com.github.xjtuwsn.cranemq.common.command.payloads.resp.MQUpdateTopicResponse;
import com.github.xjtuwsn.cranemq.common.command.types.*;
import com.github.xjtuwsn.cranemq.common.constant.MQConstant;
import com.github.xjtuwsn.cranemq.common.consumer.ConsumerInfo;
import com.github.xjtuwsn.cranemq.common.entity.ClientType;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.github.xjtuwsn.cranemq.common.remote.RemoteClent;
import com.github.xjtuwsn.cranemq.common.route.BrokerData;
import com.github.xjtuwsn.cranemq.common.route.TopicRouteInfo;
import com.github.xjtuwsn.cranemq.common.command.FutureCommand;
import com.github.xjtuwsn.cranemq.common.command.Header;
import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.exception.CraneClientException;
import com.github.xjtuwsn.cranemq.common.utils.TopicUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.xjtuwsn.cranemq.common.remote.RemoteHook;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @project:cranemq
 * @file:RemoteClient
 * @author:wsn
 * @create:2023/09/27-14:59
 */
public class ClientInstance {

    private static final Logger log= LoggerFactory.getLogger(ClientInstance.class);
    private DefaultPullConsumerImpl defaultPullConsumer;
    private LoadBalanceStrategy loadBalanceStrategy = new RandomStrategy();

    private RemoteHook hook;
    private RemoteClent remoteClent;
    private ExecutorService asyncSendThreadPool;
    private ExecutorService orderSendThreadPool;
    private ExecutorService parallelCreateService;
    private String registryAddress;
    private String clientId;
    private ScheduledExecutorService retryService;
    private ScheduledExecutorService timerService;
    private AtomicInteger state;    /** 0:未启动，1:正在启动，2:启动完成**/

    private int coreSize = 10;

    private int maxSize = 22;

    private volatile ConcurrentHashMap<String, TopicRouteInfo> topicTable = new ConcurrentHashMap<>();
    // brokerName: [id : address]
    private ConcurrentHashMap<String, HashMap<Integer, String>> brokerAddressTable = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, DefaultMQProducerImpl> producerRegister = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, DefaultPullConsumerImpl> pullConsumerRegister = new ConcurrentHashMap<>();
    // group : push
    private ConcurrentHashMap<String, DefaultPushConsumerImpl> pushConsumerRegister = new ConcurrentHashMap<>();

    private AtomicInteger clinetNumber;
    private static volatile ConcurrentHashMap<String, WrapperFutureCommand> requestTable = new ConcurrentHashMap<>();
    private volatile Semaphore updateSemphore = new Semaphore(2);
    private Random random = new Random();

    private RebalanceService rebalanceService;

    private PullMessageService pullMessageService;

    private OffsetManager offsetManager;

    public ClientInstance() {
        this.state = new AtomicInteger(0);
        this.clinetNumber = new AtomicInteger(0);
        this.remoteClent = new RemoteClent();
        this.rebalanceService = new RebalanceService(this);
        this.pullMessageService = new PullMessageService(this);
        this.offsetManager = new LocalOffsetManager(this);

    }

    public void start() {
        if (this.state.get() == 2) {
            log.info("Has alread statrted");
            return;
        }
        if (this.state.get() == 1) {
            log.info("Another client has called the start method");
            while (!this.state.compareAndSet(2, 2)) {
            }
            return;
        }
        this.state.set(1);

        this.remoteClent.registerHook(hook);
        this.registerProcessors();
        this.remoteClent.start();
        this.registryAddress = this.pickOneRegistryAddress();
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
        this.orderSendThreadPool = new ThreadPoolExecutor(1, 1, 60L,
                TimeUnit.SECONDS, new LinkedBlockingDeque<>(10000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "Ordered send thread");
                    }
                }, new ThreadPoolExecutor.AbortPolicy());
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
        this.rebalanceService.start();
        this.pullMessageService.start();
        this.state.set(2);
    }

    private void registerProcessors() {
        if (this.producerRegister != null && this.producerRegister.size() != 0) {
            this.remoteClent.registerProcessor(ClientType.PRODUCER, new PruducerProcessor(this));
        }
        if (this.pullConsumerRegister != null && this.pullConsumerRegister.size() != 0
                || this.pushConsumerRegister != null && this.pushConsumerRegister.size() != 0) {

            this.remoteClent.registerProcessor(ClientType.CONSUMER, new ConsumerProcessor(this));
        }
        this.remoteClent.registerProcessor(ClientType.BOTH, new CommonProcessor(this));
    }
    private String pickOneRegistryAddress() {
        String address = "";
        List<String> addrs = null;
        if (this.producerRegister.size() != 0) {
            log.info("This is a producer client instance");
            addrs = this.producerRegister.values().stream().map(DefaultMQProducerImpl::getRegisteryAddress)
                    .map(Arrays::asList).reduce(new ArrayList<>(), (a, b) -> {
                        a.addAll(b);
                        return a;
                    });

        } else if (this.pullConsumerRegister.size() != 0) {
            log.info("This is a pull consumer client instance");
            addrs = this.pullConsumerRegister.values().stream().map(DefaultPullConsumerImpl::getRegistryAddress)
                    .map(Arrays::asList).reduce(new ArrayList<>(), (a, b) -> {
                        a.addAll(b);
                        return a;
                    });
        } else if (this.pushConsumerRegister.size() != 0) {
            log.info("This is a push consumer client instance");
            addrs = this.pushConsumerRegister.values().stream().map(DefaultPushConsumerImpl::getRegisteryAddress)
                    .map(Arrays::asList).reduce(new ArrayList<>(), (a, b) -> {
                        a.addAll(b);
                        return a;
                    });
        }
        int size = addrs.size();
        address = addrs.get(this.random.nextInt(size));
        return address;
    }
    private void startScheduleTast() {
        // 每30s向注册中心更新路由
        this.timerService.scheduleAtFixedRate(() -> {
            log.info("Fetech topic router info from registery");
            this.fetchRouteInfo();
        }, 0, 1000 * 30, TimeUnit.MILLISECONDS);
        this.timerService.scheduleAtFixedRate(() -> {
            log.info("Clean expired remote broker");
            this.cleanExpired();
        }, 300, 1000 * 60, TimeUnit.MILLISECONDS);
        this.timerService.scheduleAtFixedRate(() -> {
            log.info("Send heartbeat to all brokers");
            this.sendHeartBeatToBroker();
        }, 500, 20 * 1000, TimeUnit.MILLISECONDS);
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
    private String selectProducedQueueAndChangeHeader(final WrapperFutureCommand wrappered, String topic) {
        MQSelector selector = wrappered.getSelector();
        MessageQueue queue = null;
        if (wrappered.getQueuePicked() == null) {
            if (selector == null) {
                queue = this.loadBalanceStrategy.getNextQueue(topic, this.topicTable.get(topic));
            } else {
                TopicRouteInfo info = this.topicTable.get(topic);
                List<MessageQueue> messageQueues = info.getAllQueueList();
                queue = selector.select(messageQueues, wrappered.getArg());
            }
        } else {
            queue = wrappered.getQueuePicked();
        }

        if (queue == null) {
            throw new CraneClientException("Queue select error");
        }
        String address = this.brokerAddressTable.get(queue.getBrokerName()).get(MQConstant.MASTER_ID);
        if (StrUtil.isEmpty(address)) {
            log.error("Cannot find address when getting {} broker", topic);
        }
        this.setQueueToRequest(wrappered, queue);
        return address;
    }
    private void setQueueToRequest(final WrapperFutureCommand wrappered, MessageQueue queue) {
        PayLoad payLoad = wrappered.getFutureCommand().getRequest().getPayLoad();
        if (payLoad instanceof MQProduceRequest) {
            ((MQProduceRequest) payLoad).setWriteQueue(queue);
        } else if (payLoad instanceof MQBachProduceRequest) {
            ((MQBachProduceRequest) payLoad).setWriteQueue(queue);
        }

    }
    public void sendQueryMsgToAllBrokers(Set<String> topics, String group) {
        WrapperFutureCommand wrapperFutureCommand = new WrapperFutureCommand(null, "");
        // 初始化topic路由
        for (String topic : topics) {
            wrapperFutureCommand.setTopic(topic);
            sendAdaptor(wrapperFutureCommand, null, false);
        }
        // 发往不同broker
        Set<String> addrs = new HashSet<>(topics.stream().map(e -> {
            TopicRouteInfo info = topicTable.get(e);
            return info.getBrokerAddresses();
        }).reduce(new ArrayList<>(), (a, b) -> {
            a.addAll(b);
            return a;
        }));

        for (String address : addrs) {
            String id = TopicUtil.generateUniqueID();
            Header header = new Header(RequestType.QUERY_INFO, RpcType.SYNC, id);
            PayLoad payLoad = new MQReblanceQueryRequest(this.clientId, group, topics);
            RemoteCommand remoteCommand = new RemoteCommand(header, payLoad);
            FutureCommand futureCommand = new FutureCommand(remoteCommand);
            WrapperFutureCommand wrappered = new WrapperFutureCommand(futureCommand, "");
            requestTable.put(id, wrappered);
            this.remoteClent.invoke(address, remoteCommand);
            try {
                futureCommand.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

    }
    public void sendOffsetToBroker(RemoteCommand remoteCommand, Set<String> brokerNames) {

        for (String name : brokerNames) {
            HashMap<Integer, String> map = this.brokerAddressTable.get(name);
            if (map != null) {
                String addr = map.get(MQConstant.MASTER_ID);
                this.asyncSendThreadPool.execute(() -> {
                    this.remoteClent.invoke(addr, remoteCommand);
                });
            }
        }
    }
    public void sendHeartBeatToBroker() {

        MQHeartBeatRequest heartBeatRequest = new MQHeartBeatRequest(this.clientId);
        Set<String> producerGroup = new HashSet<>();
        Set<ConsumerInfo> consumerInfos = new HashSet<>();
        for (Map.Entry<String, DefaultMQProducerImpl> entry : this.producerRegister.entrySet()) {
            DefaultMQProducerImpl value = entry.getValue();
            producerGroup.add(value.getDefaultMQProducer().getGroup());
        }
        heartBeatRequest.setProducerGroup(producerGroup);
        for (Map.Entry<String, DefaultPushConsumerImpl> entry : this.pushConsumerRegister.entrySet()) {
            DefaultPushConsumerImpl value = entry.getValue();
            ConsumerInfo info = new ConsumerInfo();
            info.setConsumerGroup(entry.getKey());
            info.setStartConsume(value.getStartConsume());
            info.setMessageModel(value.getMessageModel());
            info.setSubscriptionInfos(value.getSubscriptionInfos());
            consumerInfos.add(info);
        }
        heartBeatRequest.setConsumerGroup(consumerInfos);
        Header header = new Header(RequestType.HEARTBEAT, RpcType.ONE_WAY, TopicUtil.generateUniqueID());
        RemoteCommand remoteCommand = new RemoteCommand(header, heartBeatRequest);
        if (this.brokerAddressTable.isEmpty()) {
            return;
        }
        for (Map.Entry<String, HashMap<Integer, String>> entry : this.brokerAddressTable.entrySet()) {
            HashMap<Integer, String> addrs = entry.getValue();
            String address = addrs.get(MQConstant.MASTER_ID);
            this.asyncSendThreadPool.execute(() -> {
                this.remoteClent.invoke(address, remoteCommand);
            });
        }
    }

    public PullResult sendPullSync(final WrapperFutureCommand wrappered) {
        FutureCommand futureCommand = wrappered.getFutureCommand();
        if (this.hook != null) {
            this.hook.beforeMessage();
        }
        this.sendAdaptor(wrappered, null, true);
        PullResult result = null;
        RemoteCommand response = null;
        try {
            response = futureCommand.get();
            log.info("Sync method get response: {}", response);
        } catch (InterruptedException | ExecutionException | CraneClientException e) {
            result = new PullResult(AcquireResultType.ERROR);
            log.warn("Sync Request has retred for max time");
        }
        if (result == null) {
            result = new PullResult((MQSimplePullResponse) response.getPayLoad());

        }
        return result;
    }

    public SendResult sendMessageSync(final WrapperFutureCommand wrappered, boolean isOneWay) {
        FutureCommand futureCommand = wrappered.getFutureCommand();
        if (this.hook != null && !wrappered.isToRegistery()) {
            this.hook.beforeMessage();
        }

        this.sendAdaptor(wrappered, null, true);
        if (isOneWay) {
            return null;
        }
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

        this.sendAdaptor(wrappered, wrappered.getCallback(), true);
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
        } else if (response.getHeader().getCommandType() == ResponseType.LOCK_RESPONSE) {
            MQLockRespnse mqLockRespnse = (MQLockRespnse) response.getPayLoad();
            if (!mqLockRespnse.isSuccess()) {
                result.setResultType(SendResultType.SERVER_ERROR);
            }
        }
        return result;
    }
    private void sendAdaptor(final WrapperFutureCommand wrappered, SendCallback callback, boolean proceed) {
        String topic = wrappered.getTopic();
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
        if (!proceed) {
            return;
        }
        if (wrappered.getSelector() == null) {
            this.asyncSendThreadPool.execute(() -> {
                this.sendCore(wrappered, callback, this.asyncSendThreadPool);
            });
        } else {

            this.orderSendThreadPool.execute(() -> {
                this.sendCore(wrappered, callback, this.orderSendThreadPool);
            });
        }

    }
    private void sendCore(final WrapperFutureCommand wrappered, SendCallback callback, ExecutorService executorService) {
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
                    executorService.execute(() -> {
                        log.info("Request {} do retry", correlationID);
                        sendCore(newWrappered, callback, executorService);
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
        for (Map.Entry<String, DefaultPullConsumerImpl> entry : this.pullConsumerRegister.entrySet()) {
            DefaultPullConsumerImpl impl = entry.getValue();
            if (impl != null) {
                topicList.addAll(impl.getTopicSet());
            }
        }
        for (Map.Entry<String, DefaultPushConsumerImpl> entry : this.pushConsumerRegister.entrySet()) {
            DefaultPushConsumerImpl impl = entry.getValue();
            if (impl != null) {
                topicList.addAll(impl.getTopicSet());
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
    public void registerPullConsumer(DefaultPullConsumerImpl defaultPullConsumer) {
        int order = this.clinetNumber.getAndIncrement();
        this.pullConsumerRegister.put("pullconsumer-" + order, defaultPullConsumer);
    }
    public void registerPushConsumer(String group, DefaultPushConsumerImpl defaultPushConsumer) {
        int order = this.clinetNumber.getAndIncrement();
        this.pushConsumerRegister.put(group, defaultPushConsumer);
    }
    public void setLoadBalanceStrategy(LoadBalanceStrategy loadBalanceStrategy) {
        this.loadBalanceStrategy = loadBalanceStrategy;
    }
    public List<MessageQueue> listQueues(Set<String> topics) {
        for (String topic : topics) {
            this.getTopicInfoSync(topic);
        }
        return topics.stream().map(e -> {
            TopicRouteInfo info = topicTable.get(e);
            if (info != null) {
                return info.getAllQueueList();
            }
            return null;
        }).filter(Objects::nonNull).reduce(new ArrayList<>(), (a, b) -> {
            a.addAll(b);
            return a;
        });
    }
    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public RebalanceService getRebalanceService() {
        return rebalanceService;
    }
    public DefaultPushConsumerImpl getPushConsumerByGroup(String group) {
        return pushConsumerRegister.get(group);
    }

    public String getClientId() {
        return clientId;
    }

    public PullMessageService getPullMessageService() {
        return pullMessageService;
    }

    public OffsetManager getOffsetManager() {
        return offsetManager;
    }
}
