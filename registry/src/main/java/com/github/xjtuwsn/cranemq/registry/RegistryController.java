package com.github.xjtuwsn.cranemq.registry;

import com.github.xjtuwsn.cranemq.common.remote.RemoteServer;
import com.github.xjtuwsn.cranemq.common.remote.enums.HandlerType;
import com.github.xjtuwsn.cranemq.common.remote.event.ChannelEventListener;
import com.github.xjtuwsn.cranemq.registry.client.ClientManager;
import com.github.xjtuwsn.cranemq.registry.core.TopicInfoHolder;
import com.github.xjtuwsn.cranemq.registry.processors.RegistryProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @project:cranemq
 * @file:RegistryController
 * @author:wsn
 * @create:2023/10/15-15:59
 */
public class RegistryController {
    private static final Logger log = LoggerFactory.getLogger(RegistryController.class);
    private int lisntenPort;

    private RemoteServer remoteServer;
    private ExecutorService queryService;
    private ExecutorService updateService;
    private TopicInfoHolder topicInfoHolder;
    private int coreSize = 10;
    private int maxSize = 16;

    public RegistryController(int lisntenPort) {
        this.lisntenPort = lisntenPort;
        this.remoteServer = new RemoteServer(lisntenPort, new ClientManager(this));
        this.remoteServer.registerProcessor(new RegistryProcessor(this));
        this.topicInfoHolder = new TopicInfoHolder();
        this.initThreadPool();
        this.registerThreadPool();
    }
    private void initThreadPool() {
        this.queryService = new ThreadPoolExecutor(coreSize, maxSize, 60L,
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(3000),
                new ThreadFactory() {
                    AtomicInteger index = new AtomicInteger(0);
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "QueryService NO." + index.getAndIncrement());
                    }
                });
        this.updateService = new ThreadPoolExecutor(coreSize, maxSize, 60L,
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(3000),
                new ThreadFactory() {
                    AtomicInteger index = new AtomicInteger(0);
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "UpdateService NO." + index.getAndIncrement());
                    }
                });
    }

    private void registerThreadPool() {
        this.remoteServer.registerThreadPool(HandlerType.QUERY_INFO, queryService);
        this.remoteServer.registerThreadPool(HandlerType.UPDATE_INFO, updateService);
    }

    public void start() {
        this.remoteServer.start();
    }

    public void shutdown() {
        this.remoteServer.shutdown();
    }

    public TopicInfoHolder getTopicInfoHolder() {
        return topicInfoHolder;
    }

    public RemoteServer getRemoteServer() {
        return remoteServer;
    }
}
