package com.github.xjtuwsn.cranemq.common.command.payloads.resp;

import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import lombok.*;

import java.util.Map;
import java.util.Set;

/**
 * @project:cranemq
 * @file:MQRebalanceQueryResponse
 * @author:wsn
 * @create:2023/10/10-19:27
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class MQRebalanceQueryResponse implements PayLoad {

    private String group;

    private Set<String> clients;

    private Map<MessageQueue, Long> offsets;
}
