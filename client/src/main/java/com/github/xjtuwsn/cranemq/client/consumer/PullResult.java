package com.github.xjtuwsn.cranemq.client.consumer;

import com.github.xjtuwsn.cranemq.common.command.payloads.resp.MQSimplePullResponse;
import com.github.xjtuwsn.cranemq.common.command.types.PullResultType;
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

    private PullResultType pullResultType;
    private long nextOffset;

    private List<ReadyMessage> messages;

    public PullResult(PullResultType pullResultType) {
        this.pullResultType = PullResultType.ERROR;
    }
    public PullResult(MQSimplePullResponse mqSimplePullResponse) {
        this.pullResultType = mqSimplePullResponse.getResultType();
        this.nextOffset = mqSimplePullResponse.getNextOffset();
        this.messages = mqSimplePullResponse.getMessages();

    }
}
