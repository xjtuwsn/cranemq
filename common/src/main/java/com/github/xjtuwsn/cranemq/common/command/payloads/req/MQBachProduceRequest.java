package com.github.xjtuwsn.cranemq.common.command.payloads.req;

import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import lombok.*;
import com.github.xjtuwsn.cranemq.common.entity.Message;

import java.util.List;

/**
 * @project:cranemq
 * @file:MQBachProduceRequest
 * @author:wsn
 * @create:2023/09/27-19:39
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class MQBachProduceRequest implements PayLoad {
    private List<Message> messages;
    private MessageQueue writeQueue;

    public MQBachProduceRequest(List<Message> messages) {
        this.messages = messages;
    }
}
