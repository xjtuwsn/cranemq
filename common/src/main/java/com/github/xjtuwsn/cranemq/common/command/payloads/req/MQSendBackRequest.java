package com.github.xjtuwsn.cranemq.common.command.payloads.req;

import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.entity.ReadyMessage;
import lombok.*;

import java.util.List;

/**
 * @project:cranemq
 * @file:MQSendBackRequest
 * @author:wsn
 * @create:2023/10/21-15:20
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class MQSendBackRequest implements PayLoad {

    private List<ReadyMessage> readyMessages;
    private String groupName;
}
