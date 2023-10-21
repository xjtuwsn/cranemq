package com.github.xjtuwsn.cranemq.extension.impl;

import com.github.xjtuwsn.cranemq.common.remote.ReadableRegistry;
import com.github.xjtuwsn.cranemq.common.remote.RegistryCallback;
import com.github.xjtuwsn.cranemq.common.remote.RemoteHook;
import com.github.xjtuwsn.cranemq.common.route.BrokerData;
import com.github.xjtuwsn.cranemq.common.route.QueueData;
import com.github.xjtuwsn.cranemq.common.route.TopicRouteInfo;
import com.github.xjtuwsn.cranemq.extension.AbstractZkRegistry;
import com.github.xjtuwsn.cranemq.extension.ZkNodeData;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @project:cranemq
 * @file:ZkReadableRegistry
 * @author:wsn
 * @create:2023/10/17-16:18
 */
public class ZkReadableRegistry extends AbstractZkRegistry implements ReadableRegistry {
    private static final Logger log = LoggerFactory.getLogger(ZkReadableRegistry.class);
    //--------      /cranemq/topic1/brokerName/brokerId --- [address:queueData]
    public ZkReadableRegistry(String address) {
        super(address);
    }

    @Override
    public TopicRouteInfo fetchRouteInfo(String topic) {
        try {
            if (topic == null) {
                return null;
            }
            return readTopic(topic);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public void fetchRouteInfo(String topic, RegistryCallback callback) {
        try {
            TopicRouteInfo info = readTopic(topic);
            if (callback != null) {
                callback.onRouteInfo(info);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    private TopicRouteInfo readTopic(String topic) {
        String path = ROOT + "/" + topic;
        List<String> brokers = readBrokers(path);
        if (brokers == null || brokers.isEmpty()) {
            return null;
        }

        List<BrokerData> brokerDatas = new ArrayList<>();
        for (String broker : brokers) {
            List<String> ids = readIds(path + "/" + broker);
            Map<Integer, String> addresses = new HashMap<>();
            Map<Integer, QueueData> queueDataMap = new HashMap<>();

            if (ids == null || ids.isEmpty()) {
                continue;
            }

            for (String id : ids) {

                int brokerId = Integer.parseInt(id);
                ZkNodeData data = readData(path + "/" + broker + "/" + id);

                if (data == null) {
                    continue;
                }
                String address = data.getAddress();
                QueueData queueData = data.getQueueData();
                addresses.put(brokerId, address);
                queueDataMap.put(brokerId, queueData);

            }

            if (!addresses.isEmpty() && !queueDataMap.isEmpty()) {
                brokerDatas.add(new BrokerData(broker, addresses, queueDataMap));
            }
        }
        if (brokerDatas.isEmpty()) {
            return null;
        }
        return new TopicRouteInfo(topic, brokerDatas);
    }
    private List<String> readBrokers(String path) {
        try {
            return client.getChildren().forPath(path);
        } catch (Exception e) {
            return null;
        }
    }
    private List<String> readIds(String path) {
        try {
            return client.getChildren().forPath(path);
        } catch (Exception e) {
            return null;
        }
    }
    private ZkNodeData readData(String path) {
        try {
            byte[] bytes = client.getData().forPath(path);
            return byteToData(bytes);
        } catch (Exception e) {
            return null;
        }
    }
}
