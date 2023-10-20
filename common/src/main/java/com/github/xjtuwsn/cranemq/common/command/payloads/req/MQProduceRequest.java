package com.github.xjtuwsn.cranemq.common.command.payloads.req;

import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import lombok.*;
import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.entity.Message;

import java.io.Serializable;

/**
 * @project:cranemq
 * @file:MessageProduceRequest
 * @author:wsn
 * @create:2023/09/26-21:07
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class MQProduceRequest implements PayLoad, Serializable {
    private Message message;
    private MessageQueue writeQueue;

    private long delay;
    public MQProduceRequest(Message message) {
        this.message = message;
    }

    public MQProduceRequest(Message message, MessageQueue writeQueue) {
        this.message = message;
        this.writeQueue = writeQueue;
    }

    public MQProduceRequest(Message message, long delay) {
        this.message = message;
        this.delay = delay;
    }
}
