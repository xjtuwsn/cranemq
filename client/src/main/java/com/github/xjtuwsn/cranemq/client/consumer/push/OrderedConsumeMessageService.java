package com.github.xjtuwsn.cranemq.client.consumer.push;

import com.github.xjtuwsn.cranemq.client.remote.WrapperFutureCommand;
import com.github.xjtuwsn.cranemq.client.consumer.impl.DefaultPushConsumerImpl;
import com.github.xjtuwsn.cranemq.client.consumer.listener.MessageListener;
import com.github.xjtuwsn.cranemq.client.consumer.listener.OrderedMessageListener;
import com.github.xjtuwsn.cranemq.client.hook.InnerCallback;
import com.github.xjtuwsn.cranemq.common.command.FutureCommand;
import com.github.xjtuwsn.cranemq.common.command.Header;
import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.command.payloads.req.MQLockRequest;
import com.github.xjtuwsn.cranemq.common.command.payloads.resp.MQLockRespnse;
import com.github.xjtuwsn.cranemq.common.command.types.LockType;
import com.github.xjtuwsn.cranemq.common.command.types.RequestType;
import com.github.xjtuwsn.cranemq.common.command.types.RpcType;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.github.xjtuwsn.cranemq.common.entity.ReadyMessage;
import com.github.xjtuwsn.cranemq.common.utils.TopicUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @project:cranemq
 * @file:OrderedConsumeMessageService
 * @author:wsn
 * @create:2023/10/12-14:36
 * 顺序消费服务
 */
public class OrderedConsumeMessageService extends AbstractReputMessageService {
    private static final Logger log = LoggerFactory.getLogger(OrderedConsumeMessageService.class);

    private ExecutorService asyncDispatchService;
    private OrderedMessageListener listener;
    private ScheduledExecutorService renewLockTimer;

    private String group;

    public OrderedConsumeMessageService(MessageListener listener, DefaultPushConsumerImpl defaultPushConsumer) {
        super(defaultPushConsumer);
        this.group = defaultPushConsumer.getDefaultPushConsumer().getConsumerGroup();
        this.listener = (OrderedMessageListener) listener;
        this.asyncDispatchService = new ThreadPoolExecutor(COUSMER_CORE_SIZE, COUSMER_MAX_SIZE, 60L, TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(5000),
                new ThreadFactory() {
                    AtomicInteger index = new AtomicInteger(0);
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "AsyncDispatchService NO." + index.getAndIncrement());
                    }
                });
        this.renewLockTimer = new ScheduledThreadPoolExecutor(1);
    }
    @Override
    public void start() {
        // 定期续期分布式锁
        this.renewLockTimer.scheduleAtFixedRate(() -> {
            renewLock();
        }, 2 * 1000, 20 * 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() {

    }

    /**
     * 提交顺序消息供消费
     * @param messageQueue
     * @param snapShot
     * @param messages
     */
    @Override
    public void submit(MessageQueue messageQueue, BrokerQueueSnapShot snapShot, List<ReadyMessage> messages) {

        if (messageQueue != null && snapShot != null && messages != null) {
            this.asyncDispatchService.execute(() -> {
                while (true) {
                    // 首先获取到该队列的本地锁，只有一个线程能获得锁并跳出循环
                    final Object lock = defaultPushConsumer.getMessageQueueLock().acquireLock(messageQueue);
                    if (lock == null) {
                        log.error("Acquire lock error");
                        return;
                    }
                    synchronized (lock) {
                        // 获得锁的线程比较当前消息的偏移是否和期望偏移一致
                        long curOffset = messages.get(0).getOffset();
                        long expectOffset = this.defaultPushConsumer.getOffsetManager().readOffset(messageQueue, group);
                        // 一致的话跳出循环
                        if (curOffset == expectOffset || expectOffset == -1) {
                            break;
                        }
                        // 不一致就释放锁，等其它线程先消费
                    }
                }

                boolean result = false;
                if (listener != null && !snapShot.isExpired()) {
                    // 获取本地快照的锁，防止被rebalance掉队列
                    snapShot.tryLock();
                    // 消费
                    result = listener.consume(messages);
                }
                // 看是否消费成功
                if (result) {

                    long lowestOffset = snapShot.removeMessages(messages);
                    log.info("Consume message finished, will record offset {}", lowestOffset);
                    this.defaultPushConsumer.getOffsetManager().record(messageQueue, lowestOffset, group);
                } else {
                    this.sendMessageBackToBroker(messages, true);
                }
                snapShot.releaseLock();

            });
        }
    }


    /**
     * 向broker进行锁的续期
     */
    private void renewLock() {
        ConcurrentHashMap<MessageQueue, BrokerQueueSnapShot> map =
                this.defaultPushConsumer.getClientInstance().getRebalanceService().getQueueSnap(group);
        if (map == null) {
            return;
        }
        for (Map.Entry<MessageQueue, BrokerQueueSnapShot> inner : map.entrySet()) {
            MessageQueue messageQueue = inner.getKey();
            BrokerQueueSnapShot snapShot = inner.getValue();

            Header header = new Header(RequestType.LOCK_REQUEST, RpcType.ASYNC, TopicUtil.generateUniqueID());
            PayLoad payLoad = new MQLockRequest(group, messageQueue,
                    this.defaultPushConsumer.getClientInstance().getClientId(), LockType.RENEW);
            RemoteCommand remoteCommand = new RemoteCommand(header, payLoad);
            FutureCommand futureCommand = new FutureCommand(remoteCommand);
            WrapperFutureCommand wrappered = new WrapperFutureCommand(
                    futureCommand, 2, -1,
                    new InnerCallback() {
                        @Override
                        public void onResponse(RemoteCommand remoteCommand) {
                            MQLockRespnse mqLockRespnse = (MQLockRespnse) remoteCommand.getPayLoad();
                            if (mqLockRespnse.isSuccess()) {
                                snapShot.renewLockTime();
                            }
                        }
                    }, messageQueue.getTopic());
            wrappered.setQueuePicked(messageQueue);
            this.defaultPushConsumer.getClientInstance().sendMessageAsync(wrappered);
        }
    }
}
