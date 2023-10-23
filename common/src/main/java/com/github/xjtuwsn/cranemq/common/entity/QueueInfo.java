package com.github.xjtuwsn.cranemq.common.entity;

import com.github.xjtuwsn.cranemq.common.constant.MQConstant;
import lombok.*;

/**
 * @project:cranemq
 * @file:QueueInfo
 * @author:wsn
 * @create:2023/10/23-19:33
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class QueueInfo {

    private String topic;

    private int queueId;

    private long writePointer;

    private long flushPointer;

    private long messageNumber;

    private String lastModifiedTime;
    // 0-default
    // 1-normal
    // 2-retry
    // 3-delay
    // 4-dlq
    private int type;

    public QueueInfo(String topic, int queueId, long writePointer, long flushPointer, long messageNumber, String lastModifiedTime) {
        this.topic = topic;
        this.queueId = queueId;
        this.writePointer = writePointer;
        this.flushPointer = flushPointer;
        this.messageNumber = messageNumber;
        this.lastModifiedTime = lastModifiedTime;
        this.initType();
    }

    private void initType() {
        if (this.topic.startsWith(MQConstant.DLQ_PREFIX)) {
            type = 4;
        } else if (this.topic.startsWith(MQConstant.RETRY_PREFIX)) {
            type = 2;
        } else if (this.topic.startsWith(MQConstant.DELAY_TOPIC_NAME)) {
            type = 3;
        } else if (this.topic.equals(MQConstant.DEFAULT_TOPIC_NAME)) {
            type = 0;
        } else {
            type = 1;
        }
    }
}
