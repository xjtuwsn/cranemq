package com.github.xjtuwsn.cranemq.client.producer;

import java.util.List;

/**
 * @project:cranemq
 * @file:MQProducerInner
 * @author:wsn
 * @create:2023/09/27-14:37
 */
public interface MQProducerInner {
    List<String> getBrokerAddress();

    String fetechOrCreateTopic(int queueNumber);
}
