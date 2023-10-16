package com.github.xjtuwsn.cranemq.common.remote;

import com.github.xjtuwsn.cranemq.common.route.QueueData;
import com.github.xjtuwsn.cranemq.common.route.TopicRouteInfo;

import java.util.Map;

/**
 * @project:cranemq
 * @file:WritableRegistry
 * @author:wsn
 * @create:2023/10/16-11:13
 */
public interface WritableRegistry extends RemoteService {
    void uploadRouteInfo(String brokerName, int brokerId, String address, Map<String, QueueData> queueDatas);

    void append();
}
