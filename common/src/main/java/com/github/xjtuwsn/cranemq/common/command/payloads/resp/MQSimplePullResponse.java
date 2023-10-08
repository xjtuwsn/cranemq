package com.github.xjtuwsn.cranemq.common.command.payloads.resp;

import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.command.types.AcquireResultType;
import com.github.xjtuwsn.cranemq.common.entity.ReadyMessage;
import lombok.*;

import java.util.List;

/**
 * @project:cranemq
 * @file:MQSimplePullResponse
 * @author:wsn
 * @create:2023/10/07-19:20
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class MQSimplePullResponse implements PayLoad {
    private AcquireResultType resultType;
    private List<ReadyMessage> messages;
    private long nextOffset;

    public MQSimplePullResponse(AcquireResultType resultType) {
        this.resultType = resultType;
    }
}
