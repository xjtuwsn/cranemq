package com.github.xjtuwsn.cranemq.client.consumer;

import com.github.xjtuwsn.cranemq.client.WrapperFutureCommand;
import com.github.xjtuwsn.cranemq.client.consumer.impl.DefaultPushConsumerImpl;
import com.github.xjtuwsn.cranemq.client.consumer.push.BrokerQueueSnapShot;
import com.github.xjtuwsn.cranemq.client.consumer.push.PullMessageService;
import com.github.xjtuwsn.cranemq.client.consumer.push.PullRequest;
import com.github.xjtuwsn.cranemq.client.consumer.rebalance.QueueAllocation;
import com.github.xjtuwsn.cranemq.client.remote.ClientInstance;
import com.github.xjtuwsn.cranemq.common.command.FutureCommand;
import com.github.xjtuwsn.cranemq.common.command.Header;
import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.command.payloads.req.MQReblanceQueryRequest;
import com.github.xjtuwsn.cranemq.common.command.payloads.resp.MQNotifyChangedResponse;
import com.github.xjtuwsn.cranemq.common.command.types.RequestType;
import com.github.xjtuwsn.cranemq.common.command.types.RpcType;
import com.github.xjtuwsn.cranemq.common.consumer.MessageModel;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.github.xjtuwsn.cranemq.common.utils.TopicUtil;
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
    public void resetGroupConsumer(String group, Set<String> clients) {
        this.groupConsumerTable.put(group, clients);
    }

    private void rebalanceWithQuery() {

    }
    // TODO 启动时立即rebalance，但可能和心跳导致的冲突，consumer管理部分需要再看
    // TODO 定时进行rebalance，定时向broker保存offset，消费有bug
    public void rebalanceNow(String group) {
        log.info("Do rebalance now {}", group);
        DefaultPushConsumerImpl consumer = clientInstance.getPushConsumerByGroup(group);

        if (consumer == null) {
            log.warn("No such consumer, {}", group);
            return;
        }


        // 该消费者组所有topic
        Set<String> topicSet = consumer.getTopicSet();
        this.clientInstance.sendQueryMsgToAllBrokers(topicSet, group);
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
        List<MessageQueue> allocated = null;
        if (consumer.getMessageModel() == MessageModel.BRODERCAST) {
            allocated = queues;
        } else if (consumer.getMessageModel() == MessageModel.CLUSTER) {
            allocated = queueAllocation.allocate(queues, groupConsumerTable.get(group), clientInstance.getClientId());
        }

        log.error("Got queues {}", allocated);
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
                removeMessageQueue(removedMq, removedSnap, group);
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
            pullRequest.setOffset(clientInstance.getPushConsumerByGroup(group).getOffsetManager().readOffset(newQueue, group));
            hashMap.put(newQueue, brokerQueueSnapShot);
            this.clientInstance.getPullMessageService().putRequestNow(pullRequest);
        }
    }
    private void removeMessageQueue(MessageQueue removedMq, BrokerQueueSnapShot removedSnap, String group) {
        // TODO 保存message queue 的offset到broker 上锁
        removedSnap.markExpired();

        this.clientInstance.getPushConsumerByGroup(group).getOffsetManager().persistOffset();

        queueSnap.get(group).remove(removedMq);
    }
    public void start() {
    }
    public void shutdown() {

    }
}
