package com.github.xjtuwsn.cranemq.common.command.payloads.resp;

import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.route.TopicRouteInfo;
import lombok.*;

/**
 * @project:cranemq
 * @file:MQUpdateTopicResponse
 * @author:wsn
 * @create:2023/09/28-22:08
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class MQQueryTopicResponse implements PayLoad {
    private String topic;
    private TopicRouteInfo routeInfo;
}
