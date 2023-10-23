package com.github.xjtuwsn.cranemq.common.entity;

import com.github.xjtuwsn.cranemq.common.constant.MQConstant;
import lombok.ToString;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;

/**
 * @project:cranemq
 * @file:MessageQueue
 * @author:wsn
 * @create:2023/09/29-22:03
 */
@ToString
public class MessageQueue implements Serializable, Comparable {
    private String topic;
    private String brokerName;
    private int queueId;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MessageQueue that = (MessageQueue) o;
        return queueId == that.queueId && Objects.equals(topic, that.topic) && Objects.equals(brokerName, that.brokerName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, brokerName, queueId);
    }

    public MessageQueue() {
    }

    public MessageQueue(String brokerName, int queueId) {
        this.brokerName = brokerName;
        this.queueId = queueId;
    }

    public MessageQueue(String topic, String brokerName, int queueId) {
        this.topic = topic;
        this.brokerName = brokerName;
        this.queueId = queueId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    @Override
    public int compareTo(Object o) {
        MessageQueue other = (MessageQueue) o;
        if (topic.startsWith(MQConstant.RETRY_PREFIX)) {
            return 1;
        }
        if (brokerName.equals(other.getBrokerName())) {
            if (topic.equals(other.getTopic())) {
                return queueId - other.getQueueId();
            }
            return topic.compareTo(other.getTopic());
        }

        return brokerName.compareTo(other.getBrokerName());
    }
}
