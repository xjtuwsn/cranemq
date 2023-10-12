package com.github.xjtuwsn.cranemq.common.command.payloads.req;

import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.command.types.LockType;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import lombok.*;

/**
 * @project:cranemq
 * @file:MQLockRequest
 * @author:wsn
 * @create:2023/10/12-16:12
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class MQLockRequest implements PayLoad {

    private String group;
    private MessageQueue messageQueue;
    private String clientId;
    private LockType lockType;
}
