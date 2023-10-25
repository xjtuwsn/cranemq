package com.github.xjtuwsn.cranemq.extension.impl;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.github.xjtuwsn.cranemq.common.remote.WritableRegistry;
import com.github.xjtuwsn.cranemq.common.route.QueueData;
import com.github.xjtuwsn.cranemq.common.route.TopicRouteInfo;
import com.github.xjtuwsn.cranemq.extension.AbstractNacosRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @project:cranemq
 * @file:NacosWritableRegistry
 * @author:wsn
 * @create:2023/10/25-10:47
 */
public class NacosWritableRegistry extends AbstractNacosRegistry implements WritableRegistry {
    private static final Logger log = LoggerFactory.getLogger(NacosWritableRegistry.class);
    public NacosWritableRegistry(String address) {
        super(address);
    }

    @Override
    public void uploadRouteInfo(String brokerName, int brokerId, String address, Map<String, QueueData> queueDatas) {
        for (Map.Entry<String, QueueData> entry : queueDatas.entrySet()) {
            String topic = entry.getKey();
            QueueData queueData = entry.getValue();
            Instance instance = new Instance();
            Map<String, String> map = new HashMap<>();
            map.put(BROKER_NAME, brokerName);
            map.put(BROKER_ID, String.valueOf(brokerId));
            map.put(ADDRESS, address);
            map.put(QUEUE_NUMBER, String.valueOf(queueData.getWriteQueueNums()));
            instance.setMetadata(map);
            try {
                namingService.registerInstance(topic, GROUP_NAME, instance);
            } catch (NacosException e) {
                log.error("Topic {} data upload failed", topic);
            }
        }

    }

    @Override
    public void append() {

    }
}
