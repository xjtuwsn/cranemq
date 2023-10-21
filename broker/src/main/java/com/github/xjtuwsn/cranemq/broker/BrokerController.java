package com.github.xjtuwsn.cranemq.broker;

import com.github.xjtuwsn.cranemq.broker.client.ClientHousekeepingService;
import com.github.xjtuwsn.cranemq.broker.client.ClientLockMananger;
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
import com.github.xjtuwsn.cranemq.extension.impl.ZkWritableRegistry;
import org.checkerframework.checker.units.qual.C;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @project:cranemq
 * @file:BrokerController
 * @author:wsn
 * @create:2023/10/02-10:18
 */
public class BrokerController implements GeneralStoreService {
    private BrokerConfig brokerConfig;
    private PersistentConfig persistentConfig;
    private RemoteServer remoteServer;
    private ConsumerOffsetManager offsetManager;
    private MessageStoreCenter messageStoreCenter;

    private HoldRequestService holdRequestService;

    private ConsumerGroupManager consumerGroupManager;
    private ClientLockMananger clientLockMananger;
    private ExecutorService producerMessageService;
    private ExecutorService createTopicService;
    private ExecutorService simplePullService;
    private ExecutorService handleHeartBeatService;
    private ExecutorService handlePullService;
    private ExecutorService handleOffsetService;

    private ExecutorService sendBackService;
    private ScheduledExecutorService saveOffsetService;

    private ScheduledExecutorService heartBeatSendService;
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
        this.clientLockMananger = new ClientLockMananger();
        if (this.brokerConfig.getRegistryType() == RegistryType.DEFAULT) {
            this.writableRegistry = new SimpleWritableRegistry(this);
        } else if (this.brokerConfig.getRegistryType() == RegistryType.ZOOKEEPER) {
            this.writableRegistry = new ZkWritableRegistry(this.brokerConfig.getRegistrys());
        }
        this.brokerConfig.initRetry();
        this.initThreadPool();
        this.registerThreadPool();
        return true;
    }
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
        this.producerMessageService = new ThreadPoolExecutor(coreSize, maxSize,
                60L, TimeUnit.SECONDS, new LinkedBlockingDeque<>(10000),
                new ThreadFactory() {
                    AtomicInteger idx = new AtomicInteger(0);
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "Producer Message Service NO." + idx.getAndIncrement());
                    }
                });
        this.createTopicService = new ThreadPoolExecutor(coreSize / 3, maxSize / 3,
                60L, TimeUnit.SECONDS, new LinkedBlockingDeque<>(5000),
                new ThreadFactory() {
                    AtomicInteger idx = new AtomicInteger(0);
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "Create Topic Thread NO." + idx.getAndIncrement());
                    }
                });
        this.handleHeartBeatService = new ThreadPoolExecutor(coreSize / 3, maxSize / 3,
                60L, TimeUnit.SECONDS, new LinkedBlockingDeque<>(8000),
                new ThreadFactory() {
                    AtomicInteger idx = new AtomicInteger(0);
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "Handle HeartBeat Service NO." + idx.getAndIncrement());
                    }
                });
        this.simplePullService = new ThreadPoolExecutor(coreSize / 2, maxSize / 2,
                60L, TimeUnit.SECONDS, new LinkedBlockingDeque<>(8000),
                new ThreadFactory() {
                    AtomicInteger idx = new AtomicInteger(0);
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "Siple pull Service NO." + idx.getAndIncrement());
                    }
                });
        this.handlePullService = new ThreadPoolExecutor(coreSize / 2, maxSize / 2,
                60L, TimeUnit.SECONDS, new LinkedBlockingDeque<>(8000),
                new ThreadFactory() {
                    AtomicInteger idx = new AtomicInteger(0);
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "Pull Service NO." + idx.getAndIncrement());
                    }
                });
        this.handleOffsetService = new ThreadPoolExecutor(coreSize / 3, maxSize / 3,
                60L, TimeUnit.SECONDS, new LinkedBlockingDeque<>(3000),
                new ThreadFactory() {
                    AtomicInteger idx = new AtomicInteger(0);
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "Record Offset Service NO." + idx.getAndIncrement());
                    }
                });
        this.sendBackService = new ThreadPoolExecutor(coreSize / 3, maxSize / 3,
                60L, TimeUnit.SECONDS, new LinkedBlockingDeque<>(3000),
                new ThreadFactory() {
                    AtomicInteger idx = new AtomicInteger(0);
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "Send Back Service NO." + idx.getAndIncrement());
                    }
                });
        this.saveOffsetService = new ScheduledThreadPoolExecutor(1);
        this.heartBeatSendService = new ScheduledThreadPoolExecutor(1);
    }
    public void startScheduledTask() {
        this.saveOffsetService.scheduleAtFixedRate(() -> {
            this.offsetManager.persistOffset();
        }, 100, 5 * 1000, TimeUnit.MILLISECONDS);

        this.heartBeatSendService.scheduleAtFixedRate(() -> {
            this.updateRegistry();
        }, 0, 30 * 1000, TimeUnit.MILLISECONDS);
    }

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

    public ClientLockMananger getClientLockMananger() {
        return clientLockMananger;
    }
}
