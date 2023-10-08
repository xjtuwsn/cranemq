package com.github.xjtuwsn.cranemq.common.command.payloads.resp;

import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import lombok.*;

import java.util.Set;

/**
 * @project:cranemq
 * @file:MQNotifyChangedResponse
 * @author:wsn
 * @create:2023/10/08-19:42
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class MQNotifyChangedResponse implements PayLoad {
    private String consumerGroup;
    private Set<String> clients;
}
