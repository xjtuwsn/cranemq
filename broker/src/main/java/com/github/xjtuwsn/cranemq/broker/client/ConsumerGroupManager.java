package com.github.xjtuwsn.cranemq.broker.client;

import com.github.xjtuwsn.cranemq.common.consumer.ConsumerInfo;

/**
 * @project:cranemq
 * @file:ClientManager
 * @author:wsn
 * @create:2023/10/09-21:46
 */
public interface ConsumerGroupManager {

    ConsumerInfo getGroupProperity(String group);
}
