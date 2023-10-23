package com.github.xjtuwsn.cranemq.common.entity;

import lombok.*;

/**
 * @project:cranemq
 * @file:MessageConsume
 * @author:wsn
 * @create:2023/10/07-17:02
 */

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class ReadyMessage extends Message {
    private String brokerName;
    private int queueId;
    private long offset;
    private int retry;
    public ReadyMessage(String brokerName, int queueId, long offset, Message message, int retry) {
        super(message);
        this.brokerName = brokerName;
        this.queueId = queueId;
        this.offset = offset;
        this.retry = retry;
    }

    public boolean matchs(String topic, String tag) {
        if (!topic.equals(this.getTopic()) || !"*".equals(tag) && !tag.equals(this.getTag())) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "ReadyMessage{" +
                "brokerName='" + brokerName + '\'' +
                ", topic=" + getTopic() +
                ", queueId=" + queueId +
                ", offset=" + offset +
                ", retry=" + retry +
                '}';
    }
}
