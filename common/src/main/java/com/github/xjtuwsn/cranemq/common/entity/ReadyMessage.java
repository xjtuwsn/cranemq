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
@ToString
public class ReadyMessage extends Message {
    private String brokerName;
    private int queueId;
    private long offset;
    public ReadyMessage(String brokerName, int queueId, long offset, Message message) {
        super(message);
        this.brokerName = brokerName;
        this.queueId = queueId;
        this.offset = offset;
    }
}
