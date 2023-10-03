package com.github.xjtuwsn.cranemq.broker;

import com.github.xjtuwsn.cranemq.broker.client.ClientHousekeepingService;
import com.github.xjtuwsn.cranemq.broker.enums.HandlerType;
import com.github.xjtuwsn.cranemq.broker.remote.RemoteServer;
import com.github.xjtuwsn.cranemq.broker.store.CommitLog;
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
public class BrokerController {
    private BrokerConfig brokerConfig;
    private PersistentConfig persistentConfig;
    private RemoteServer remoteServer;
    private MessageStoreCenter messageStoreCenter;
    private ExecutorService producerMessageService;
    private ExecutorService createTopicService;
    private ExecutorService handleHeartBeatService;
    private int coreSize = 10;
    private int maxSize = 20;
    public BrokerController() {

    }

    public BrokerController(BrokerConfig brokerConfig, PersistentConfig persistentConfig) {
        this.brokerConfig = brokerConfig;
        this.persistentConfig = persistentConfig;
    }
    public boolean initialize() {
        this.remoteServer = new RemoteServer(brokerConfig.getPort(),
                this, new ClientHousekeepingService(this));
        this.messageStoreCenter = new MessageStoreCenter(this);
        this.initThreadPool();
        this.registerThreadPool();
        return true;
    }
    public void registerThreadPool() {
        this.remoteServer.registerThreadPool(HandlerType.PRODUCER_REQUEST, this.producerMessageService);
        this.remoteServer.registerThreadPool(HandlerType.CREATE_TOPIC, this.createTopicService);
        this.remoteServer.registerThreadPool(HandlerType.HEARTBEAT_REQUEST, this.handleHeartBeatService);
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
        this.createTopicService = new ThreadPoolExecutor(coreSize / 2, maxSize / 2,
                60L, TimeUnit.SECONDS, new LinkedBlockingDeque<>(5000),
                new ThreadFactory() {
                    AtomicInteger idx = new AtomicInteger(0);
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "Create Topic Thread NO." + idx.getAndIncrement());
                    }
                });
        this.handleHeartBeatService = new ThreadPoolExecutor(coreSize / 2, maxSize / 2,
                60L, TimeUnit.SECONDS, new LinkedBlockingDeque<>(5000),
                new ThreadFactory() {
                    AtomicInteger idx = new AtomicInteger(0);
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "Handle HeartBeat Service NO." + idx.getAndIncrement());
                    }
                });
    }
    public void start() {
        this.remoteServer.start();
        this.messageStoreCenter.start();
    }
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