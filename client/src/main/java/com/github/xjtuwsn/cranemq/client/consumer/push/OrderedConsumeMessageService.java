package com.github.xjtuwsn.cranemq.client.consumer.push;

import com.github.xjtuwsn.cranemq.client.WrapperFutureCommand;
import com.github.xjtuwsn.cranemq.client.consumer.impl.DefaultPushConsumerImpl;
import com.github.xjtuwsn.cranemq.client.consumer.listener.CommonMessageListener;
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
                new LinkedBlockingDeque<>(2000),
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
        this.renewLockTimer.scheduleAtFixedRate(() -> {
            renewLock();
        }, 2 * 1000, 20 * 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() {

    }

    @Override
    public void submit(MessageQueue messageQueue, BrokerQueueSnapShot snapShot, List<ReadyMessage> messages) {

        if (messageQueue != null && snapShot != null && messages != null) {
            this.asyncDispatchService.execute(() -> {
                while (true) {
                    final Object lock = defaultPushConsumer.getMessageQueueLock().acquireLock(messageQueue);
                    if (lock == null) {
                        log.error("Acquire lock error");
                        return;
                    }
                    synchronized (lock) {
                        long curOffset = messages.get(0).getOffset();
                        long expectOffset = this.defaultPushConsumer.getOffsetManager().readOffset(messageQueue, group);
                        if (curOffset == expectOffset || expectOffset == -1) {
                            break;
                        }
                    }
                }

                boolean result = false;
                if (listener != null && !snapShot.isExpired()) {
                    snapShot.tryLock();
                    result = listener.consume(messages);
                    snapShot.releaseLock();
                }
                if (result) {
                    // log.info("Consume message finished");
                    long lowestOfsset = snapShot.removeMessages(messages);
                    this.defaultPushConsumer.getOffsetManager().record(messageQueue, lowestOfsset, group);
                }
            });
        }
    }


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
