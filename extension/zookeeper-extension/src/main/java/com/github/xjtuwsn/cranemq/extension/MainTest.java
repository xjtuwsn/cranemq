package com.github.xjtuwsn.cranemq.extension;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.xjtuwsn.cranemq.common.remote.ReadableRegistry;
import com.github.xjtuwsn.cranemq.common.remote.WritableRegistry;
import com.github.xjtuwsn.cranemq.common.route.QueueData;
import com.github.xjtuwsn.cranemq.common.route.TopicRouteInfo;
import com.github.xjtuwsn.cranemq.extension.impl.ZkReadableRegistry;
import com.github.xjtuwsn.cranemq.extension.impl.ZkWritableRegistry;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @project:cranemq
 * @file:MainTest
 * @author:wsn
 * @create:2023/10/17-15:12
 */
public class MainTest {
    public static String address = "192.168.227.137:2181";
    public static String path = "/test";
    public static void main(String[] args) throws Exception {
        WritableRegistry writableRegistry = new ZkWritableRegistry(address);
        ReadableRegistry readableRegistry = new ZkReadableRegistry(address);
        readableRegistry.start();
        writableRegistry.start();
        String address = "123.123.121.12:1212";
        String topic = "topic1";
        String brokerName = "broker1";
        int brokerId = 0;
        Map<String, QueueData> map = new HashMap<>();
        map.put(topic, new QueueData(brokerName, 4, 4));
        // writableRegistry.uploadRouteInfo(brokerName, brokerId, address, map);

        TopicRouteInfo info = readableRegistry.fetchRouteInfo("topic1");
        System.out.println(info);
        readableRegistry.shutdown();
        writableRegistry.shutdown();
    }

}
