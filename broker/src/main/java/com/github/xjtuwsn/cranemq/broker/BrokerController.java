package com.github.xjtuwsn.cranemq.broker;

import com.github.xjtuwsn.cranemq.broker.client.ClientHousekeepingService;
import com.github.xjtuwsn.cranemq.broker.client.ClientLockManager;
import com.github.xjtuwsn.cranemq.broker.client.ConsumerGroupManager;
import com.github.xjtuwsn.cranemq.broker.offset.ConsumerOffsetManager;
import com.github.xjtuwsn.cranemq.broker.processors.ServerProcessor;
import com.github.xjtuwsn.cranemq.broker.push.HoldRequestService;
import com.github.xjtuwsn.cranemq.broker.registry.SimpleWritableRegistry;
import com.github.xjtuwsn.cranemq.common.remote.WritableRegistry;
import com.github.xjtuwsn.cranemq.common.remote.enums.HandlerType;
import com.github.xjtuwsn.cranemq.common.remote.RemoteServer;
import com.github.xjtuwsn.cranemq.broker.store.GeneralStoreService;
import com.github.xjtuwsn.cranemq.broker.store.MessageStoreCenter;
import com.github.xjtuwsn.cranemq.broker.store.PersistentConfig;
import com.github.xjtuwsn.cranemq.common.config.BrokerConfig;
import com.github.xjtuwsn.cranemq.common.remote.enums.RegistryType;
import com.github.xjtuwsn.cranemq.common.remote.event.ChannelEventListener;
import com.github.xjtuwsn.cranemq.common.utils.NetworkUtil;
import com.github.xjtuwsn.cranemq.extension.impl.NacosWritableRegistry;
import com.github.xjtuwsn.cranemq.extension.impl.ZkWritableRegistry;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import javax.annotation.Resource;
import java.util.concurrent.*;

/**
 * @project:cranemq
 * @file:BrokerController
 * @author:wsn
 * @create:2023/10/02-10:18
 */

/**
 * broker主体，包含大部分服务
 * @author wsn
 */
public class BrokerController implements GeneralStoreService, ApplicationContextAware {
    // broker配置
    private BrokerConfig brokerConfig;
    // 持久化配置
    private PersistentConfig persistentConfig;
    // netty服务端
    private RemoteServer remoteServer;
    // 消费者偏移管理
    private ConsumerOffsetManager offsetManager;
    // 消息存储管理
    private MessageStoreCenter messageStoreCenter;

    // 长连接管理
    private HoldRequestService holdRequestService;

    // 消费者组管理
    private ConsumerGroupManager consumerGroupManager;
    // 分布式锁管理
    private ClientLockManager clientLockManager;
    // 不同处理器对应的线程池
    @Resource
    private ExecutorService producerMessageService;
    @Resource
    private ExecutorService createTopicService;
    @Resource
    private ExecutorService simplePullService;
    @Resource
    private ExecutorService handleHeartBeatService;
    @Resource
    private ExecutorService handlePullService;
    @Resource
    private ExecutorService handleOffsetService;

    @Resource
    private ExecutorService sendBackService;
    // 定时持久化位移
    private ScheduledExecutorService saveOffsetService;

    // 定时向registry发送心跳
    private ScheduledExecutorService heartBeatSendService;
    // 远程注册中心
    private WritableRegistry writableRegistry;
    private int coreSize = 10;
    private int maxSize = 20;
    public BrokerController() {

    }

    public BrokerController(BrokerConfig brokerConfig, PersistentConfig persistentConfig) {
        this.brokerConfig = brokerConfig;
        this.persistentConfig = persistentConfig;
    }
    public boolean initialize() {
        this.consumerGroupManager = new ClientHousekeepingService(this);
        this.remoteServer = new RemoteServer(brokerConfig.getPort(), (ChannelEventListener) consumerGroupManager);
        this.remoteServer.registerProcessor(new ServerProcessor(this, remoteServer));
        this.messageStoreCenter = new MessageStoreCenter(this);
        this.offsetManager = new ConsumerOffsetManager(this);
        this.holdRequestService = new HoldRequestService(this);
        this.clientLockManager = new ClientLockManager();
        // 根据注册中心类型判断初始化哪个
        if (this.brokerConfig.getRegistryType() == RegistryType.DEFAULT) {
            this.writableRegistry = new SimpleWritableRegistry(this);
        } else if (this.brokerConfig.getRegistryType() == RegistryType.ZOOKEEPER) {
            this.writableRegistry = new ZkWritableRegistry(this.brokerConfig.getRegistrys());
        } else if (this.brokerConfig.getRegistryType() == RegistryType.NACOS) {
            this.writableRegistry = new NacosWritableRegistry(this.brokerConfig.getRegistrys());
        }
        this.brokerConfig.initRetry();
        this.initThreadPool();
        this.registerThreadPool();
        return true;
    }

    /**
     * 注册不同请求对应的线程池
     */
    public void registerThreadPool() {
        this.remoteServer.registerThreadPool(HandlerType.PRODUCER_REQUEST, this.producerMessageService);
        this.remoteServer.registerThreadPool(HandlerType.CREATE_TOPIC, this.createTopicService);
        this.remoteServer.registerThreadPool(HandlerType.HEARTBEAT_REQUEST, this.handleHeartBeatService);
        this.remoteServer.registerThreadPool(HandlerType.SIMPLE_PULL, this.simplePullService);
        this.remoteServer.registerThreadPool(HandlerType.PULL, this.handlePullService);
        this.remoteServer.registerThreadPool(HandlerType.RECORD_OFFSET, this.handleOffsetService);
        this.remoteServer.registerThreadPool(HandlerType.SEND_BACK, this.sendBackService);
    }
    private void initThreadPool() {
        this.saveOffsetService = new ScheduledThreadPoolExecutor(1);
        this.heartBeatSendService = new ScheduledThreadPoolExecutor(1);
    }

    /**
     * 开启定时任务
     */
    public void startScheduledTask() {
        this.saveOffsetService.scheduleAtFixedRate(() -> {
            this.offsetManager.persistOffset();
        }, 100, 5 * 1000, TimeUnit.MILLISECONDS);

        this.heartBeatSendService.scheduleAtFixedRate(() -> {
            this.updateRegistry();
        }, 0, 30 * 1000, TimeUnit.MILLISECONDS);
    }

    /**
     * 向注册中心更新本地topic路由信息
     */
    public void updateRegistry() {
        this.writableRegistry.uploadRouteInfo(this.brokerConfig.getBrokerName(), this.brokerConfig.getBrokerId(),
                NetworkUtil.getLocalAddress() + ":" + this.brokerConfig.getPort(),
                this.messageStoreCenter.getAllQueueData());
    }
    @Override
    public void start() {
        this.remoteServer.start();
        this.messageStoreCenter.start();


        this.offsetManager.start();
        this.holdRequestService.start();
        this.writableRegistry.start();
        this.startScheduledTask();
        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
    }
    @Override
    public void close() {
        this.remoteServer.shutdown();
        this.messageStoreCenter.close();
        this.holdRequestService.shutdown();
        this.writableRegistry.shutdown();
    }

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    public PersistentConfig getPersistentConfig() {
        return persistentConfig;
    }

    public MessageStoreCenter getMessageStoreCenter() {
        return messageStoreCenter;
    }

    public ConsumerOffsetManager getOffsetManager() {
        return offsetManager;
    }

    public HoldRequestService getHoldRequestService() {
        return holdRequestService;
    }

    public ConsumerGroupManager getConsumerGroupManager() {
        return consumerGroupManager;
    }

    public ClientLockManager getClientLockMananger() {
        return clientLockManager;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        System.out.println("-------------");
        Object bean = applicationContext.getBean("producerMessageService");
        System.out.println(bean);
        System.out.println("------------");
    }
}
