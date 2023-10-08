package com.github.xjtuwsn.cranemq.client.consumer;

import com.github.xjtuwsn.cranemq.common.command.payloads.resp.MQSimplePullResponse;
import com.github.xjtuwsn.cranemq.common.command.types.AcquireResultType;
import com.github.xjtuwsn.cranemq.common.entity.ReadyMessage;
import lombok.*;

import java.util.List;

/**
 * @project:cranemq
 * @file:PullResult
 * @author:wsn
 * @create:2023/10/07-10:33
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class PullResult {

    private AcquireResultType acquireResultType;
    private long nextOffset;

    private List<ReadyMessage> messages;

    public PullResult(AcquireResultType acquireResultType) {
        this.acquireResultType = AcquireResultType.ERROR;
    }
    public PullResult(MQSimplePullResponse mqSimplePullResponse) {
        this.acquireResultType = mqSimplePullResponse.getResultType();
        this.nextOffset = mqSimplePullResponse.getNextOffset();
        this.messages = mqSimplePullResponse.getMessages();

    }
}
