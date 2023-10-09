package com.github.xjtuwsn.cranemq.broker;

import com.github.xjtuwsn.cranemq.broker.client.ClientHousekeepingService;
import com.github.xjtuwsn.cranemq.broker.offset.ConsumerOffsetManager;
import com.github.xjtuwsn.cranemq.broker.processors.ServerProcessor;
import com.github.xjtuwsn.cranemq.common.remote.enums.HandlerType;
import com.github.xjtuwsn.cranemq.common.remote.RemoteServer;
import com.github.xjtuwsn.cranemq.broker.store.GeneralStoreService;
import com.github.xjtuwsn.cranemq.broker.store.MessageStoreCenter;
import com.github.xjtuwsn.cranemq.broker.store.PersistentConfig;
import com.github.xjtuwsn.cranemq.common.config.BrokerConfig;

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
    private ExecutorService producerMessageService;
    private ExecutorService createTopicService;
    private ExecutorService simplePullService;
    private ExecutorService handleHeartBeatService;
    private ExecutorService handlePullService;
    private ScheduledExecutorService saveOffsetService;
    private int coreSize = 10;
    private int maxSize = 20;
    public BrokerController() {

    }

    public BrokerController(BrokerConfig brokerConfig, PersistentConfig persistentConfig) {
        this.brokerConfig = brokerConfig;
        this.persistentConfig = persistentConfig;
    }
    public boolean initialize() {
        this.remoteServer = new RemoteServer(brokerConfig.getPort(), new ClientHousekeepingService(this));
        this.remoteServer.registerProcessor(new ServerProcessor(this, remoteServer));
        this.messageStoreCenter = new MessageStoreCenter(this);
        this.offsetManager = new ConsumerOffsetManager(this);
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
        this.saveOffsetService = new ScheduledThreadPoolExecutor(1);
    }
    public void startScheduledTask() {
        this.saveOffsetService.scheduleAtFixedRate(() -> {
            this.offsetManager.persistOffset();
        }, 100, 5 * 1000, TimeUnit.MILLISECONDS);
    }
    @Override
    public void start() {
        this.remoteServer.start();
        this.messageStoreCenter.start();

        this.offsetManager.start();


        this.startScheduledTask();
    }
    @Override
    public void close() {

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
}
