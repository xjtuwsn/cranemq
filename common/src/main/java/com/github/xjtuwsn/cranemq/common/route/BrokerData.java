package com.github.xjtuwsn.cranemq.common.route;

import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;

/**
 * @project:cranemq
 * @file:TopicData
 * @author:wsn
 * @create:2023/09/28-20:05
 */
@ToString
@NoArgsConstructor
@Setter
public class BrokerData implements Serializable {
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
