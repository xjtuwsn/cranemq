package com.github.xjtuwsn.cranemq.client.producer.balance;

import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.github.xjtuwsn.cranemq.common.exception.CraneClientException;
import com.github.xjtuwsn.cranemq.common.route.TopicRouteInfo;

/**
 * @project:cranemq
 * @file:LoadBalanceStrategy
 * @author:wsn
 * @create:2023/09/29-21:58
 */
public interface LoadBalanceStrategy {

    MessageQueue getNextQueue(String topic, TopicRouteInfo info) throws CraneClientException;
}
