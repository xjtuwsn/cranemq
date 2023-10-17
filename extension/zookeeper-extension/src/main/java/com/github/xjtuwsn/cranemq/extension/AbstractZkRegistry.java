package com.github.xjtuwsn.cranemq.extension;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.xjtuwsn.cranemq.common.remote.RemoteHook;
import com.github.xjtuwsn.cranemq.common.remote.RemoteService;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @project:cranemq
 * @file:AbstractZkRegistry
 * @author:wsn
 * @create:2023/10/17-16:15
 */
public abstract class AbstractZkRegistry implements RemoteService {
    private static final Logger log = LoggerFactory.getLogger(AbstractZkRegistry.class);
    protected static final String ROOT = "/cranemq";
    protected static final int DATA_LEVEL = 4;

    private String address;
    protected CuratorFramework client;
    protected RetryPolicy retryPolicy;
    public AbstractZkRegistry(String address) {
        this.address = address;
        retryPolicy = new ExponentialBackoffRetry(1000, 3);
        client = CuratorFrameworkFactory.builder()
                .connectString(address)
                .retryPolicy(retryPolicy)
                .build();
        log.error("User zookeeper as registry, address is {}", address);
    }

    protected ZkNodeData byteToData(byte[] bytes) {
        String str = new String(bytes);
        return JSONObject.parseObject(str, ZkNodeData.class);
    }

    protected byte[] dataToByte(ZkNodeData data) {
        String jsonString = JSON.toJSONString(data);
        return jsonString.getBytes();
    }
    @Override
    public void start() {
        if (this.client != null) {
            this.client.start();
            log.info("Zookeeper registry connect success {}", address);
        }
    }

    @Override
    public void shutdown() {
        if (this.client != null) {
            this.client.close();
        }
    }

    @Override
    public void registerHook(RemoteHook hook) {

    }
}
