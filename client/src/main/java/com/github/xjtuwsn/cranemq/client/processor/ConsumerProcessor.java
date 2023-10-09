package com.github.xjtuwsn.cranemq.client.processor;

import com.github.xjtuwsn.cranemq.client.WrapperFutureCommand;
import com.github.xjtuwsn.cranemq.client.consumer.PullResult;
import com.github.xjtuwsn.cranemq.client.producer.result.SendResult;
import com.github.xjtuwsn.cranemq.client.remote.ClientInstance;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.command.payloads.resp.MQNotifyChangedResponse;
import com.github.xjtuwsn.cranemq.common.command.payloads.resp.MQPullMessageResponse;
import com.github.xjtuwsn.cranemq.common.remote.RemoteHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

/**
 * @project:cranemq
 * @file:ConsumerProcessor
 * @author:wsn
 * @create:2023/10/07-19:45
 */
public class ConsumerProcessor extends AbstractClientProcessor {
    private static final Logger log = LoggerFactory.getLogger(ConsumerProcessor.class);
    public ConsumerProcessor(ClientInstance clientInstance) {
        super(clientInstance);
    }

    @Override
    public void processSimplePullResponse(RemoteCommand remoteCommand, ExecutorService asyncHookService,
                                          RemoteHook hook) {

        this.parseResponseWithRetry(remoteCommand, asyncHookService);
        if (hook != null) {
            if (asyncHookService != null) {
                asyncHookService.execute(hook::afterMessage);
            } else {
                hook.afterMessage();
            }
        }

    }

    @Override
    public void processNotifyChangedResponse(RemoteCommand remoteCommand, ExecutorService asyncHookService) {
        MQNotifyChangedResponse payLoad = (MQNotifyChangedResponse) remoteCommand.getPayLoad();
        new Thread(() -> {
            this.clientInstance.getRebalanceService().updateConsumerGroup(payLoad);
        }).start();

    }

    @Override
    public void processPullResponse(RemoteCommand remoteCommand, ExecutorService asyncHookService) {
        int responseCode = remoteCommand.getHeader().getStatus();
        String correlationID = remoteCommand.getHeader().getCorrelationId();
        MQPullMessageResponse mqPullMessageResponse = (MQPullMessageResponse) remoteCommand.getPayLoad();
        WrapperFutureCommand wrappered = this.clientInstance.getWrapperFuture(correlationID);
        PullResult result = new PullResult();
        result.setMessages(mqPullMessageResponse.getMessages());
        result.setAcquireResultType(mqPullMessageResponse.getAcquireResultType());
        result.setNextOffset(mqPullMessageResponse.getNextOffset());

        if (wrappered.getPullCallback() != null) {
            asyncHookService.execute(() -> {
                wrappered.getPullCallback().onSuccess(result);
            });
        }
    }
}
