package com.github.xjtuwsn.cranemq.registry.core;

import com.github.xjtuwsn.cranemq.common.route.BrokerData;
import com.github.xjtuwsn.cranemq.common.route.QueueData;
import com.github.xjtuwsn.cranemq.common.route.TopicRouteInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @project:cranemq
 * @file:TopicInfoHolder
 * @author:wsn
 * @create:2023/10/15-16:45
 */
public class TopicInfoHolder {
    private static final Logger log = LoggerFactory.getLogger(TopicInfoHolder.class);
    // topic: [brokerName: brokerData]
    private ConcurrentHashMap<String, ConcurrentHashMap<String, BrokerData>> topicTable = new ConcurrentHashMap<>();
    // brokerName: [topics]
    private ConcurrentHashMap<String, Set<String>> brokerTopicTable = new ConcurrentHashMap<>();

    public TopicRouteInfo getTopicRouteInfo(String topic) {
        if (!topicTable.containsKey(topic)) {
            return null;
        }
        ConcurrentHashMap<String, BrokerData> map = topicTable.get(topic);
        List<BrokerData> collect = new ArrayList<>(map.values());
        TopicRouteInfo info = new TopicRouteInfo(topic, collect);
        return info;
    }

    public void update(String brokerName, int id, Map<String, QueueData> queueDatas, String address) {
        if (brokerTopicTable.containsKey(brokerName)) {
            brokerTopicTable.put(brokerName, new HashSet<>());
        }
        log.info("broker: {}, id: {}, data: {}, address: {}",brokerName, id, queueDatas, address);
        for (Map.Entry<String, QueueData> outter : queueDatas.entrySet()) {
            String topic = outter.getKey();
            QueueData queueData = outter.getValue();

            ConcurrentHashMap<String, BrokerData> temp = new ConcurrentHashMap<>();
            ConcurrentHashMap<String, BrokerData> origin = topicTable.putIfAbsent(topic, temp);
            if (origin == null) {
                origin = temp;
            }
            BrokerData tempData = new BrokerData(brokerName);
            BrokerData originData = origin.putIfAbsent(brokerName, tempData);
            if (originData == null) {
                originData = tempData;
            }
            originData.putAddress(id, address);
            originData.putQueueData(id, queueData);
        }
    }

    public void removerBrokerData(String brokerName, int brokerId) {
        Set<String> topics = brokerTopicTable.get(brokerName);
        if (topics == null) {
            return;
        }
        for (String topic : topics) {
            ConcurrentHashMap<String, BrokerData> map = topicTable.get(topic);
            if (map == null) {
                continue;
            }
            BrokerData brokerData = map.get(brokerName);
            if (brokerData == null) {
                continue;
            }
            brokerData.remove(brokerId);
        }
    }
}
