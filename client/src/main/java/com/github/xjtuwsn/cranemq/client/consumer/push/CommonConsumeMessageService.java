package com.github.xjtuwsn.cranemq.client.consumer.push;

import com.github.xjtuwsn.cranemq.client.consumer.impl.DefaultPushConsumerImpl;
import com.github.xjtuwsn.cranemq.client.consumer.listener.CommonMessageListener;
import com.github.xjtuwsn.cranemq.client.consumer.listener.MessageListener;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.github.xjtuwsn.cranemq.common.entity.ReadyMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @project:cranemq
 * @file:CommonConsumeMessageService
 * @author:wsn
 * @create:2023/10/09-11:02
 * 普通消息的消费服务
 */
public class CommonConsumeMessageService extends AbstractReputMessageService {
    private static final Logger log = LoggerFactory.getLogger(CommonConsumeMessageService.class);

    private ExecutorService asyncDispatchService;
    private CommonMessageListener listener;

    public CommonConsumeMessageService(MessageListener listener, DefaultPushConsumerImpl defaultPushConsumer) {
        super(defaultPushConsumer);
        this.listener = (CommonMessageListener) listener;
        this.asyncDispatchService = new ThreadPoolExecutor(COUSMER_CORE_SIZE, COUSMER_MAX_SIZE, 60L,
                TimeUnit.SECONDS, new LinkedBlockingDeque<>(5000),
                new ThreadFactory() {
                    AtomicInteger index = new AtomicInteger(0);
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "AsyncDispatchService NO." + index.getAndIncrement());
                    }
                });
    }
    @Override
    public void start() {

    }

    @Override
    public void shutdown() {

    }

    /**
     * 提交一批消息供消费
     * @param messageQueue
     * @param snapShot
     * @param messages
     */
    @Override
    public void submit(MessageQueue messageQueue, BrokerQueueSnapShot snapShot, List<ReadyMessage> messages) {
        if (messageQueue != null && snapShot != null && messages != null) {
            // 多线程一部消费
            this.asyncDispatchService.execute(() -> {
                boolean result = false;
                // 调用监听器
                if (listener != null) {
                    result = listener.consume(messages);
                }
                // 如果消费成功，更新位移
                if (result) {
                    // log.info("Consume message finished");
                    long lowestOfsset = snapShot.removeMessages(messages);
                    this.defaultPushConsumer.getOffsetManager().record(messageQueue, lowestOfsset,
                            this.defaultPushConsumer.getDefaultPushConsumer().getConsumerGroup());
                } else {
                    // 否则返回broker重试
                    this.sendMessageBackToBroker(messages, false);
                }
            });
        }
    }
}
