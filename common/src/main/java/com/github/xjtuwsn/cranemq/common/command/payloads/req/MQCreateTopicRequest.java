package com.github.xjtuwsn.cranemq.common.command.payloads.req;

import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.constant.MQConstant;
import lombok.*;

/**
 * @project:cranemq
 * @file:MQCreateTopicRequest
 * @author:wsn
 * @create:2023/09/30-15:12
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class MQCreateTopicRequest implements PayLoad {

    private String topic;
    private int readNumber = MQConstant.DEFAULT_QUEUE_NUMBER;
    private int wirteNumber = MQConstant.DEFAULT_QUEUE_NUMBER;

    public MQCreateTopicRequest(String topic) {
        this.topic = topic;
    }
}
