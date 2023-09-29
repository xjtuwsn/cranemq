package com.github.xjtuwsn.cranemq.common.command.payloads;

import com.github.xjtuwsn.cranemq.common.route.TopicRouteInfo;
import lombok.*;

import java.util.List;

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
public class MQUpdateTopicResponse {
    private String topic;
    private TopicRouteInfo routeInfo;
}
