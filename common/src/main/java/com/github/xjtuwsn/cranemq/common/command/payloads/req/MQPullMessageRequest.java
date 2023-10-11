package com.github.xjtuwsn.cranemq.common.command.payloads.req;

import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import lombok.*;

/**
 * @project:cranemq
 * @file:MQPushMessageRequest
 * @author:wsn
 * @create:2023/10/08-16:56
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class MQPullMessageRequest implements PayLoad {
    private String clientId;
    private String groupName;
    private MessageQueue messageQueue;
    private long offset;
    private long commitOffset;
    public String getTopic() {
        return messageQueue.getTopic();
    }

}
