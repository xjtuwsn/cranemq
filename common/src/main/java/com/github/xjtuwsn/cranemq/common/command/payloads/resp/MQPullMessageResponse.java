package com.github.xjtuwsn.cranemq.common.command.payloads.resp;

import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.command.types.AcquireResultType;
import com.github.xjtuwsn.cranemq.common.entity.ReadyMessage;
import lombok.*;

import java.util.List;

/**
 * @project:cranemq
 * @file:MQPushMessageResponse
 * @author:wsn
 * @create:2023/10/08-16:39
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Data
public class MQPullMessageResponse implements PayLoad {
    private AcquireResultType acquireResultType;
    private String groupName;
    private List<ReadyMessage> messages;
    private long nextOffset;

    public int getMessageCount() {
        if (messages == null) {
            return 0;
        }
        return messages.size();
    }
}
