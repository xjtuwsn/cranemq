package com.github.xjtuwsn.cranemq.client.remote;

import com.github.xjtuwsn.cranemq.common.exception.CraneClientException;
import com.github.xjtuwsn.cranemq.common.net.RemoteAddress;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @project:cranemq
 * @file:ClienFactory
 * @author:wsn
 * @create:2023/09/27-15:43
 */
public class ClienFactory {

    private static ClienFactory instance = new ClienFactory();
    private ConcurrentHashMap<String, RemoteClient> cache = new ConcurrentHashMap<>();

    public static ClienFactory newInstance() {
        return instance;
    }

    public RemoteClient getOrCreate(String key, RemoteAddress address) throws CraneClientException {
        RemoteClient client = null;
        if (!cache.containsKey(key)) {
            client = new RemoteClient(address);
            cache.putIfAbsent(key, client);
        }
        client = cache.get(key);
        if (client == null) {
            throw new CraneClientException("Create client error, key = " + key);
        }
        return client;
    }
}
