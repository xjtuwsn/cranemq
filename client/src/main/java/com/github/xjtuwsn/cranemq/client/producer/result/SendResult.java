package com.github.xjtuwsn.cranemq.client.producer.result;

import com.github.xjtuwsn.cranemq.common.route.TopicRouteInfo;
import lombok.*;

/**
 * @project:cranemq
 * @file:SendResult
 * @author:wsn
 * @create:2023/09/27-19:42
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class SendResult {
    private SendResultType resultType;
    private String correlationID;
    private TopicRouteInfo topicRouteInfo;
    private String topic;

    public SendResult(SendResultType resultType, String correlationID) {
        this.resultType = resultType;
        this.correlationID = correlationID;
    }
}
