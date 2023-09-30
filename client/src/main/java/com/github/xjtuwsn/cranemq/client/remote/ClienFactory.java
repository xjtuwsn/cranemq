package com.github.xjtuwsn.cranemq.client.remote;

import com.github.xjtuwsn.cranemq.client.hook.RemoteHook;
import com.github.xjtuwsn.cranemq.client.producer.impl.DefaultMQProducerImpl;
import com.github.xjtuwsn.cranemq.common.exception.CraneClientException;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @project:cranemq
 * @file:ClienFactory
 * @author:wsn
 * @create:2023/09/27-15:43
 */
public class ClienFactory {

    private static ClienFactory instance = new ClienFactory();
    private ConcurrentHashMap<String, ClientInstance> cache = new ConcurrentHashMap<>();

    public static ClienFactory newInstance() {
        return instance;
    }

    public synchronized ClientInstance getOrCreate(String key,
                                                   DefaultMQProducerImpl impl,
                                                   RemoteHook hook) throws CraneClientException {
        ClientInstance client = null;
        if (!cache.containsKey(key)) {
            client = new ClientInstance(impl);
            cache.putIfAbsent(key, client);
            client.registerHook(hook);
            client.start();
        }
        client = cache.get(key);
        if (client == null) {
            throw new CraneClientException("Create client error, key = " + key);
        }
        return client;
    }
}
