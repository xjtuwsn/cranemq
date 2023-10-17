package com.github.xjtuwsn.cranemq.common.route;

import com.github.xjtuwsn.cranemq.common.constant.MQConstant;
import lombok.*;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @project:cranemq
 * @file:TopicData
 * @author:wsn
 * @create:2023/09/28-20:05
 */
@ToString
@NoArgsConstructor
@Setter
@Getter
@AllArgsConstructor
public class BrokerData implements Serializable {
    // TODO 在集群时需要改很多
    private String brokerName;
    // 主从节点位置
    private Map<Integer, String> brokerAddressMap;
    //
    private Map<Integer, QueueData> queueDataMap;

    public BrokerData(String brokerName) {
        this.brokerName = brokerName;
        this.brokerAddressMap = new HashMap<>();
        this.queueDataMap = new HashMap<>();
    }
    public void putAddress(int brokerId, String address) {
        this.brokerAddressMap.put(brokerId, address);
    }

    public void putQueueData(int brokerId, QueueData queueData) {
        this.queueDataMap.put(brokerId, queueData);
    }
    public String getMasterAddress() {
        return this.brokerAddressMap.get(MQConstant.MASTER_ID);
    }
    public QueueData getMasterQueueData() {
        return this.queueDataMap.get(MQConstant.MASTER_ID);
    }
    public void remove(int brokerId) {
        this.brokerAddressMap.remove(brokerId);
        this.queueDataMap.remove(brokerId);
    }
}
