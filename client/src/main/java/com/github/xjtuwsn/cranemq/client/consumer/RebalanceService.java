package com.github.xjtuwsn.cranemq.client.consumer;

import com.github.xjtuwsn.cranemq.client.WrapperFutureCommand;
import com.github.xjtuwsn.cranemq.client.consumer.impl.DefaultPushConsumerImpl;
import com.github.xjtuwsn.cranemq.client.consumer.push.BrokerQueueSnapShot;
import com.github.xjtuwsn.cranemq.client.consumer.push.PullMessageService;
import com.github.xjtuwsn.cranemq.client.consumer.push.PullRequest;
import com.github.xjtuwsn.cranemq.client.consumer.rebalance.QueueAllocation;
import com.github.xjtuwsn.cranemq.client.hook.InnerCallback;
import com.github.xjtuwsn.cranemq.client.producer.result.SendResult;
import com.github.xjtuwsn.cranemq.client.producer.result.SendResultType;
import com.github.xjtuwsn.cranemq.client.remote.ClientInstance;
import com.github.xjtuwsn.cranemq.common.command.FutureCommand;
import com.github.xjtuwsn.cranemq.common.command.Header;
import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.command.payloads.req.MQLockRequest;
import com.github.xjtuwsn.cranemq.common.command.payloads.req.MQReblanceQueryRequest;
import com.github.xjtuwsn.cranemq.common.command.payloads.resp.MQLockRespnse;
import com.github.xjtuwsn.cranemq.common.command.payloads.resp.MQNotifyChangedResponse;
import com.github.xjtuwsn.cranemq.common.command.types.LockType;
import com.github.xjtuwsn.cranemq.common.command.types.RequestType;
import com.github.xjtuwsn.cranemq.common.command.types.RpcType;
import com.github.xjtuwsn.cranemq.common.consumer.MessageModel;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.github.xjtuwsn.cranemq.common.utils.TopicUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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

    private ScheduledExecutorService doRebalanceTimer;

    public RebalanceService(ClientInstance clientInstance) {
        this.clientInstance = clientInstance;
        this.doRebalanceTimer = new ScheduledThreadPoolExecutor(1);
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

        // 重分配，广播模式获得所有，集群模式进行分配
        List<MessageQueue> allocated = null;
        if (consumer.getMessageModel() == MessageModel.BRODERCAST) {
            allocated = queues;
        } else if (consumer.getMessageModel() == MessageModel.CLUSTER) {
            allocated = queueAllocation.allocate(queues, groupConsumerTable.get(group), clientInstance.getClientId());
        }
        log.error("Got queue {}", allocated);
        this.clientInstance.getPushConsumerByGroup(group).getMessageQueueLock().resetLock(allocated);

        Set<MessageQueue> allocatedSet = new HashSet<>(allocated);
        if (!queueSnap.containsKey(group)) {
            queueSnap.put(group, new ConcurrentHashMap<>());
        }
        ConcurrentHashMap<MessageQueue, BrokerQueueSnapShot> hashMap = queueSnap.get(group);
        Iterator<Map.Entry<MessageQueue, BrokerQueueSnapShot>> iterator = hashMap.entrySet().iterator();
        // 删除被分配走的队列
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


        // 新分配到的队列，如果是集群的顺序消费，先向服务端申请分布式锁，然后构建拉取请求
        for (MessageQueue newQueue : allocatedSet) {
            if (consumer.needLock()) {
                // 先申请队列的锁
                Header header = new Header(RequestType.LOCK_REQUEST, RpcType.ASYNC, TopicUtil.generateUniqueID());
                PayLoad payLoad = new MQLockRequest(group, newQueue, clientInstance.getClientId(), LockType.APPLY);
                RemoteCommand remoteCommand = new RemoteCommand(header, payLoad);
                FutureCommand futureCommand = new FutureCommand(remoteCommand);
                WrapperFutureCommand wrappered = new WrapperFutureCommand(futureCommand, 2, -1,
                        null, newQueue.getTopic());
                wrappered.setQueuePicked(newQueue);
                SendResult result = clientInstance.sendMessageSync(wrappered, false);
                if (result.getResultType() != SendResultType.SEDN_OK) {
                    log.error("Apply for message queue lock failed after 3 retry");
                    continue;
                }
            }

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

    /**
     * 从当前client中删除不再属于自己的队列信息
     * @param removedMq
     * @param removedSnap
     * @param group
     */
    private void removeMessageQueue(MessageQueue removedMq, BrokerQueueSnapShot removedSnap, String group) {
        // TODO 保存message queue 的offset到broker 上锁
        DefaultPushConsumerImpl consumer = clientInstance.getPushConsumerByGroup(group);

        // 将消费快照标记位过期，其它线程不再消费
        removedSnap.markExpired();

        // 如果是顺序消费，则需要等待当前正在消费的队列消费完成，然后释放队列锁
        if (consumer.needLock()) {
            if (removedSnap.tryLock()) {
                Header header = new Header(RequestType.LOCK_REQUEST, RpcType.ASYNC, TopicUtil.generateUniqueID());
                PayLoad payLoad = new MQLockRequest(group, removedMq, clientInstance.getClientId(), LockType.RELEASE);
                RemoteCommand remoteCommand = new RemoteCommand(header, payLoad);
                FutureCommand futureCommand = new FutureCommand(remoteCommand);
                WrapperFutureCommand wrappered = new WrapperFutureCommand(futureCommand, 2, -1,
                        null, removedMq.getTopic());
                wrappered.setQueuePicked(removedMq);
                SendResult result = clientInstance.sendMessageSync(wrappered, false);
                if (result.getResultType() != SendResultType.SEDN_OK) {
                    log.error("Release lock failed");
                    return;
                }

            } else {
                log.error("Cannot get the lock off snapshot, will remove in next rebalance");
                return;
            }
        }

        // 向服务端同步当前队列消费位移
        this.clientInstance.getPushConsumerByGroup(group).getOffsetManager().persistOffset();

        queueSnap.get(group).remove(removedMq);
    }

    public ConcurrentHashMap<MessageQueue, BrokerQueueSnapShot> getQueueSnap(String group) {
        return queueSnap.get(group);
    }

    public void start() {
        this.doRebalanceTimer.scheduleAtFixedRate(() -> {
            for (Map.Entry<String, Set<String>> entry : groupConsumerTable.entrySet()) {
                rebalanceNow(entry.getKey());
            }
        }, 10 * 1000, 20 * 1000, TimeUnit.MILLISECONDS);
    }
    public void shutdown() {

    }



}
