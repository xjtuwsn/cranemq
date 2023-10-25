package com.github.xjtuwsn.cranemq.client.remote;

import com.github.xjtuwsn.cranemq.common.remote.RemoteHook;
import com.github.xjtuwsn.cranemq.common.exception.CraneClientException;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @project:cranemq
 * @file:ClienFactory
 * @author:wsn
 * @create:2023/09/27-15:43
 * 单例产生客户端实例
 */
public class ClientFactory {

    private static ClientFactory instance = new ClientFactory();
    private ConcurrentHashMap<String, ClientInstance> cache = new ConcurrentHashMap<>();

    public static ClientFactory newInstance() {
        return instance;
    }

    public synchronized ClientInstance getOrCreate(String key,
                                                   RemoteHook hook) throws CraneClientException {
        ClientInstance client = null;
        if (!cache.containsKey(key)) {
            client = new ClientInstance();
            cache.putIfAbsent(key, client);
            client.registerHook(hook);
            client.setClientId(key);
        }
        client = cache.get(key);
        if (client == null) {
            throw new CraneClientException("Create client error, key = " + key);
        }
        return client;
    }
}
