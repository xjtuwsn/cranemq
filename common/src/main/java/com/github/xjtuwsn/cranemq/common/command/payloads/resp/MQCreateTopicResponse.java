package com.github.xjtuwsn.cranemq.common.command.payloads.resp;

import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.route.TopicRouteInfo;
import lombok.*;

/**
 * @project:cranemq
 * @file:MQCreateTopicResponse
 * @author:wsn
 * @create:2023/09/30-15:32
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class MQCreateTopicResponse implements PayLoad {
    private TopicRouteInfo singleBrokerInfo; // 只有当前集群一个broker的路由信息
}
