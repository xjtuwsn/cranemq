package com.github.xjtuwsn.cranemq.common.route;

import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
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

    public TopicRouteInfo() {
    }

    public TopicRouteInfo(String topic) {
        this.topic = topic;
    }
    public List<MessageQueue> getAllQueueList() {
        return brokerData.stream().map(BrokerData::getMasterQueueData).map(e -> {
            int number = e.getWriteQueueNums();
            List<MessageQueue> list = new ArrayList<>();
            for (int i = 0; i < number; i++) {
                MessageQueue messageQueue = new MessageQueue(topic, e.getBroker(), i);
                list.add(messageQueue);
            }
            return list;
        }).reduce(new ArrayList<>(), (a, b) -> {
             a.addAll(b);
             return a;
        });
    }
    public MessageQueue getNumberKQueue(int k) {
        int sum = 0, idx = 0, p = 0;
        for (BrokerData data : this.brokerData) {
            int cur = data.getMasterQueueData().getWriteQueueNums();
            sum += cur;
            if (k < sum) {
                idx = p;
                break;
            }
            p++;
        }
        BrokerData choose = this.getBroker(idx);
        MessageQueue queue = new MessageQueue(choose.getBrokerName(),
                choose.getMasterQueueData().getWriteQueueNums() - (sum - k));
        return queue;
    }
    public TopicRouteInfo(String topic, List<BrokerData> brokerData) {
        this.topic = topic;
        this.brokerData = brokerData;
    }
    public List<String> getBrokerAddresses() {
        return this.brokerData.stream().map(BrokerData::getMasterAddress).collect(Collectors.toList());
    }
    public int brokerNumber() {
        return this.brokerData.size();
    }
    public int getTotalWriteQueueNumber() {
        return this.brokerData.stream().mapToInt(e -> e.getMasterQueueData().getWriteQueueNums()).sum();
    }
    public BrokerData getBroker(int idx) {
        if (idx >= this.brokerData.size() || idx < 0) {
            return null;
        }
        return this.brokerData.get(idx);
    }
    public void compact(TopicRouteInfo other) {
        if (!this.topic.equals(other.getTopic())) {
            return;
        }
        if (this.brokerData == null) {
            this.brokerData = new ArrayList<>();
        }
        this.brokerData.addAll(other.getBrokerData());
    }
    public List<String> getExpiredBrokerAddress(TopicRouteInfo other) {
        if (other == null) {
            return this.brokerData.stream().map(BrokerData::getMasterAddress).collect(Collectors.toList());
        }
        Set<String> newAddress =
                other.getBrokerData().stream().map(BrokerData::getMasterAddress).collect(Collectors.toSet());
        List<String> ans = new ArrayList<>();
        if (newAddress == null) {
            return this.brokerData.stream().map(BrokerData::getMasterAddress).collect(Collectors.toList());
        }
        for (BrokerData data : this.brokerData) {
            if (!newAddress.contains(data.getMasterAddress())) {
                ans.add(data.getMasterAddress());
            }
        }
        return ans;
    }

}
