package com.github.xjtuwsn.cranemq.client.producer.balance;

import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.github.xjtuwsn.cranemq.common.exception.CraneClientException;
import com.github.xjtuwsn.cranemq.common.route.TopicRouteInfo;

import java.util.Random;

/**
 * @project:cranemq
 * @file:RandomLoadBalance
 * @author:wsn
 * @create:2023/09/29-22:00
 */
public class RandomLoadBalance implements LoadBalanceStrategy {
    private Random random = new Random();
    @Override
    public MessageQueue getNextQueue(String topic, TopicRouteInfo info) throws CraneClientException {
        int queueNumber = info.getQueueData().get(0).getWriteQueueNums();
        int picked = random.nextInt(queueNumber);
        return new MessageQueue(topic, info.getBrokerData().get(0).getBrokerName(), picked);
    }
}
