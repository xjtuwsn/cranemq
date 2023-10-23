package com.github.xjtuwsn.cranemq.common.command.payloads.req;

import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.consumer.ConsumerInfo;
import lombok.*;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * @project:cranemq
 * @file:MQHeartBeatRequest
 * @author:wsn
 * @create:2023/10/02-20:14
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class MQHeartBeatRequest implements Serializable, PayLoad {
    private String clientId;
    private Set<String> producerGroup;
    private Set<ConsumerInfo> consumerGroup;
    private boolean grayConsumer;
    public MQHeartBeatRequest(String clientId) {
        this.clientId = clientId;
        this.producerGroup = new HashSet<>();
    }
}
