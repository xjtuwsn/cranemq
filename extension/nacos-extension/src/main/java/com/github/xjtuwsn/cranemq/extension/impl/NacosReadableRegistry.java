package com.github.xjtuwsn.cranemq.extension.impl;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.github.xjtuwsn.cranemq.common.remote.ReadableRegistry;
import com.github.xjtuwsn.cranemq.common.remote.RegistryCallback;
import com.github.xjtuwsn.cranemq.common.route.BrokerData;
import com.github.xjtuwsn.cranemq.common.route.QueueData;
import com.github.xjtuwsn.cranemq.common.route.TopicRouteInfo;
import com.github.xjtuwsn.cranemq.extension.AbstractNacosRegistry;

import javax.print.attribute.standard.MediaSize;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @project:cranemq
 * @file:NacosReadableRegistry
 * @author:wsn
 * @create:2023/10/25-10:44
 */
public class NacosReadableRegistry extends AbstractNacosRegistry implements ReadableRegistry {
    public NacosReadableRegistry(String address) {
        super(address);
    }

    @Override
    public TopicRouteInfo fetchRouteInfo(String topic) {
        TopicRouteInfo info = new TopicRouteInfo(topic);
        Map<String, BrokerData> map = new HashMap<>();
        try {
            List<Instance> allInstances = namingService.getAllInstances(topic, GROUP_NAME);

            if (allInstances == null || allInstances.isEmpty()) {
                return null;
            }
            for (Instance instance : allInstances) {
                if (!instance.isHealthy()) {
                    continue;
                }
                Map<String, String> metadata = instance.getMetadata();
                if (metadata == null) {
                    continue;
                }
                String brokerName = metadata.get(BROKER_NAME);
                int brokerId = Integer.parseInt(metadata.get(BROKER_ID));
                String address = metadata.get(ADDRESS);
                int queueNumber = Integer.parseInt(metadata.get(QUEUE_NUMBER));
                QueueData queueData = new QueueData(brokerName, queueNumber, queueNumber);

                BrokerData brokerData = map.get(brokerName);
                if (brokerData == null) {
                    brokerData = new BrokerData(brokerName);
                    map.put(brokerName, brokerData);
                }

                brokerData.putAddress(brokerId, address);
                brokerData.putQueueData(brokerId, queueData);

            }

            List<BrokerData> brokerDatas = new ArrayList<>(map.values());
            info.setBrokerData(brokerDatas);
            return info;
        } catch (NacosException e) {
            return null;
        }
    }

    @Override
    public void fetchRouteInfo(String topic, RegistryCallback callback) {
        TopicRouteInfo info = fetchRouteInfo(topic);
        if (info != null) {
            callback.onRouteInfo(info);
        }
    }
}
