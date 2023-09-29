package com.github.xjtuwsn.cranemq.common.command.payloads;

import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import lombok.*;

/**
 * @project:cranemq
 * @file:MQUpdateTopicRequest
 * @author:wsn
 * @create:2023/09/28-21:50
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class MQUpdateTopicRequest implements PayLoad {
    private String topic;
}
