package com.github.xjtuwsn.cranemq.common.route;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @project:cranemq
 * @file:TopicReouteInfo
 * @author:wsn
 * @create:2023/09/28-20:00
 */
@ToString
@Setter
@Data
@Getter
public class TopicRouteInfo implements Serializable {
    private String topic;
    private List<BrokerData> brokerData;
    private List<QueueData> queueData;

    public TopicRouteInfo() {
    }

    public TopicRouteInfo(String topic, List<BrokerData> brokerData, List<QueueData> queueData) {
        this.topic = topic;
        this.brokerData = brokerData;
        this.queueData = queueData;
    }
    public List<String> getExpiredBrokerAddress(TopicRouteInfo other) {
        Set<String> newAddress =
                other.getBrokerData().stream().map(BrokerData::getBrokerAddress).collect(Collectors.toSet());
        List<String> ans = new ArrayList<>();
        if (newAddress == null) {
            return this.brokerData.stream().map(BrokerData::getBrokerAddress).collect(Collectors.toList());
        }
        for (BrokerData data : this.brokerData) {
            if (newAddress.contains(data.getBrokerAddress())) {
                ans.add(data.getBrokerAddress());
            }
        }
        return ans;
    }

}
