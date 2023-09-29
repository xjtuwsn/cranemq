package com.github.xjtuwsn.cranemq.common.route;

import java.util.List;

/**
 * @project:cranemq
 * @file:TopicData
 * @author:wsn
 * @create:2023/09/28-20:05
 */
public class BrokerData {
    // TODO 在集群时需要改很多
    private String brokerName;
    private String brokerAddress;

    public BrokerData(String brokerName) {
        this.brokerName = brokerName;
    }

    public BrokerData(String brokerName, String brokerAddress) {
        this.brokerName = brokerName;
        this.brokerAddress = brokerAddress;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public String getBrokerAddress() {
        return brokerAddress;
    }
}
