package com.github.xjtuwsn.cranemq.client.consumer;

import com.github.xjtuwsn.cranemq.client.consumer.impl.DefaultPushConsumerImpl;
import com.github.xjtuwsn.cranemq.client.consumer.push.BrokerQueueSnapShot;
import com.github.xjtuwsn.cranemq.client.consumer.push.PullMessageService;
import com.github.xjtuwsn.cranemq.client.consumer.push.PullRequest;
import com.github.xjtuwsn.cranemq.client.consumer.rebalance.QueueAllocation;
import com.github.xjtuwsn.cranemq.client.remote.ClientInstance;
import com.github.xjtuwsn.cranemq.common.command.payloads.resp.MQNotifyChangedResponse;
import com.github.xjtuwsn.cranemq.common.consumer.MessageModel;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @project:cranemq
 * @file:RebalanceService
 * @author:wsn
 * @create:2023/10/08-17:13
 */
public class RebalanceService {
    private static final Logger log = LoggerFactory.getLogger(RebalanceService.class);

    // group : [all consumer cliendId]
    private ConcurrentHashMap<String, Set<String>> groupConsumerTable = new ConcurrentHashMap<>();
    // group : [queue: snapshot]
    private ConcurrentHashMap<String, ConcurrentHashMap<MessageQueue, BrokerQueueSnapShot>> queueSnap =
            new ConcurrentHashMap<>();
    // group : [clientId: queues]

    private ClientInstance clientInstance;




    public RebalanceService(ClientInstance clientInstance) {
        this.clientInstance = clientInstance;

    }
    public void updateConsumerGroup(MQNotifyChangedResponse response) {
        String group = response.getConsumerGroup();
        groupConsumerTable.put(group, response.getClients());
        rebalanceNow(group);
    }
    private void rebalanceWithQuery() {

    }
    // TODO 测试
    private void rebalanceNow(String group) {
        log.info("Do rebalance now {}", group);
        DefaultPushConsumerImpl consumer = clientInstance.getPushConsumerByGroup(group);

        if (consumer == null) {
            log.warn("No such consumer, {}", group);
            return;
        }
        if (consumer.getMessageModel() == MessageModel.BRODERCAST) {
            return;
        }
        // 该消费者组所有topic
        Set<String> topicSet = consumer.getTopicSet();

        // 所有topic对应的队列
        List<MessageQueue> queues = clientInstance.listQueues(topicSet);
        if (queues == null || queues.size() == 0) {
            log.warn("All Topic queue is empty");
            return;
        }
        // 策略
        QueueAllocation queueAllocation = consumer.getQueueAllocation();

        if (queueAllocation == null) {
            log.warn("QueueAllocation is null");
            return;
        }

        //重分配
        List<MessageQueue> allocated
                = queueAllocation.allocate(queues, groupConsumerTable.get(group), clientInstance.getClientId());

        Set<MessageQueue> allocatedSet = new HashSet<>(allocated);
        if (!queueSnap.containsKey(group)) {
            queueSnap.put(group, new ConcurrentHashMap<>());
        }
        ConcurrentHashMap<MessageQueue, BrokerQueueSnapShot> hashMap = queueSnap.get(group);
        Iterator<Map.Entry<MessageQueue, BrokerQueueSnapShot>> iterator = hashMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<MessageQueue, BrokerQueueSnapShot> entry = iterator.next();
            if (!allocatedSet.contains(entry.getKey())) {
                MessageQueue removedMq = entry.getKey();
                BrokerQueueSnapShot removedSnap = entry.getValue();
                removeMessageQueue(removedMq, removedSnap);
                iterator.remove();
            } else {
                allocatedSet.remove(entry.getKey());
            }
        }
        for (MessageQueue newQueue : allocatedSet) {
            BrokerQueueSnapShot brokerQueueSnapShot = new BrokerQueueSnapShot();
            PullRequest pullRequest = new PullRequest();
            pullRequest.setGroupName(group);
            pullRequest.setMessageQueue(newQueue);
            pullRequest.setSnapShot(brokerQueueSnapShot);
            hashMap.put(newQueue, brokerQueueSnapShot);
            this.clientInstance.getPullMessageService().putRequestNow(pullRequest);
        }
    }
    private void removeMessageQueue(MessageQueue removedMq, BrokerQueueSnapShot removedSnap) {
        // TODO 保存message queue 的offset到broker 上锁
    }
    public void start() {
    }
    public void shutdown() {

    }
}
