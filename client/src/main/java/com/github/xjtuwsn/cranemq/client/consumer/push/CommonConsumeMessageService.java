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
 */
public class CommonConsumeMessageService extends AbstractReputMessageService {
    private static final Logger log = LoggerFactory.getLogger(CommonConsumeMessageService.class);

    private ExecutorService asyncDispatchService;
    private CommonMessageListener listener;

    public CommonConsumeMessageService(MessageListener listener, DefaultPushConsumerImpl defaultPushConsumer) {
        super(defaultPushConsumer);
        this.listener = (CommonMessageListener) listener;
        this.asyncDispatchService = new ThreadPoolExecutor(COUSMER_CORE_SIZE, COUSMER_MAX_SIZE, 60L,
                TimeUnit.SECONDS, new LinkedBlockingDeque<>(2000),
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

    @Override
    public void submit(MessageQueue messageQueue, BrokerQueueSnapShot snapShot, List<ReadyMessage> messages) {
        if (messageQueue != null && snapShot != null && messages != null) {
            this.asyncDispatchService.execute(() -> {
                boolean result = false;
                if (listener != null) {
                    result = listener.consume(messages);
                }
                if (result) {
                    // log.info("Consume message finished");
                    long lowestOfsset = snapShot.removeMessages(messages);
                    System.out.println("------------- record queue: " + messageQueue + ", offset: " + lowestOfsset);
                    this.defaultPushConsumer.getOffsetManager().record(messageQueue, lowestOfsset,
                            this.defaultPushConsumer.getDefaultPushConsumer().getConsumerGroup());
                } else {
                    this.sendMessageBackToBroker(messages, false);
                }
            });
        }
    }
}
