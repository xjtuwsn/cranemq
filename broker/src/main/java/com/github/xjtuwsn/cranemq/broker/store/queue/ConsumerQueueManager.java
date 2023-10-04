package com.github.xjtuwsn.cranemq.broker.store.queue;

import com.github.xjtuwsn.cranemq.broker.BrokerController;
import com.github.xjtuwsn.cranemq.broker.store.GeneralStoreService;
import com.github.xjtuwsn.cranemq.broker.store.PersistentConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @project:cranemq
 * @file:ConsumerQueueManager
 * @author:wsn
 * @create:2023/10/04-21:27
 */
public class ConsumerQueueManager implements GeneralStoreService {

    private static final Logger log = LoggerFactory.getLogger(ConsumerQueueManager.class);
    private ConcurrentHashMap<String, ConcurrentHashMap<Integer, ConsumerQueue>> queueTable = new ConcurrentHashMap<>();
    private BrokerController brokerController;
    private PersistentConfig persistentConfig;
    public ConsumerQueueManager(BrokerController brokerController, PersistentConfig persistentConfig) {
        this.brokerController = brokerController;
        this.persistentConfig = persistentConfig;
    }

    public void start() {

    }

    @Override
    public void close() {

    }
}
