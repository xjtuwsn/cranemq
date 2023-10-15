package com.github.xjtuwsn.cranemq.common.command.payloads.req;

import com.github.xjtuwsn.cranemq.common.route.BrokerData;
import com.github.xjtuwsn.cranemq.common.route.QueueData;
import lombok.*;

import java.util.Map;

/**
 * @project:cranemq
 * @file:MQUpdateTopicRequest
 * @author:wsn
 * @create:2023/10/15-21:36
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class MQUpdateTopicRequest {
    private String brokerName;
    private int id;
    private String address;
    private Map<String, QueueData> queueDatas;
}
