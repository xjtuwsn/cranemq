package com.github.xjtuwsn.cranemq.common.route;

import java.util.List;

/**
 * @project:cranemq
 * @file:TopicReouteInfo
 * @author:wsn
 * @create:2023/09/28-20:00
 */
public class TopicRouteInfo {
    private String topic;
    private List<BrokerData> brokerData;
    private List<QueueData> queueData;

    public String getTopic() {
        return topic;
    }

    public List<BrokerData> getBrokerData() {
        return brokerData;
    }

    public List<QueueData> getQueueData() {
        return queueData;
    }
}
