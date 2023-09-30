package com.github.xjtuwsn.cranemq.client.producer.balance;

import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.github.xjtuwsn.cranemq.common.exception.CraneClientException;
import com.github.xjtuwsn.cranemq.common.route.BrokerData;
import com.github.xjtuwsn.cranemq.common.route.TopicRouteInfo;

import java.util.Random;

/**
 * @project:cranemq
 * @file:RandomLoadBalance
 * @author:wsn
 * @create:2023/09/29-22:00
 */
public class RandomStrategy implements LoadBalanceStrategy {
    private Random random = new Random();
    @Override
    public MessageQueue getNextQueue(String topic, TopicRouteInfo info) throws CraneClientException {
        int brokerNumber = info.brokerNumber();
        int pickedBroker = random.nextInt(brokerNumber);
        BrokerData brokerData = info.getBroker(pickedBroker);
        int queueSize = brokerData.getMasterQueueData().getWriteQueueNums();
        int pickedQueue = random.nextInt(queueSize);
        return new MessageQueue(topic, brokerData.getBrokerName(), pickedQueue);
    }
}
