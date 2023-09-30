package com.github.xjtuwsn.cranemq.client.processor;

import com.github.xjtuwsn.cranemq.client.hook.InnerCallback;
import com.github.xjtuwsn.cranemq.client.hook.RemoteHook;
import com.github.xjtuwsn.cranemq.client.producer.impl.DefaultMQProducerImpl;
import com.github.xjtuwsn.cranemq.client.producer.impl.WrapperFutureCommand;
import com.github.xjtuwsn.cranemq.client.producer.result.SendResult;
import com.github.xjtuwsn.cranemq.client.producer.result.SendResultType;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.command.types.ResponseCode;
import com.github.xjtuwsn.cranemq.common.command.types.RpcType;
import com.github.xjtuwsn.cranemq.common.exception.CraneClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

/**
 * @project:cranemq
 * @file:PruducerProcessor
 * @author:wsn
 * @create:2023/09/28-11:14
 */
public class PruducerProcessor implements Processor {
    private static final Logger log = LoggerFactory.getLogger(PruducerProcessor.class);

    private DefaultMQProducerImpl mqProducerImpl;


    public PruducerProcessor(DefaultMQProducerImpl mqProducerImpl) {
        this.mqProducerImpl = mqProducerImpl;
    }

    public void processMessageProduceResopnse(RemoteCommand remoteCommand,
                                              ExecutorService asyncHookService,
                                              RemoteHook hook) {
        RpcType rpcType = remoteCommand.getHeader().getRpcType();
        int responseCode = remoteCommand.getHeader().getStatus();
        String correlationID = remoteCommand.getHeader().getCorrelationId();
        WrapperFutureCommand wrappered = this.mqProducerImpl.getWrapperFuture(correlationID);
        if (wrappered == null) {
            log.warn("Request {} has been removed before", correlationID);
            return;
        }
        if (responseCode != ResponseCode.SUCCESS) {
            log.warn("Request {} has occured a mistake, reason is {}", correlationID, responseCode);
            if (wrappered.isDone()) {
                this.mqProducerImpl.removeWrapperFuture(correlationID);
                return;
            }
            if (!wrappered.isNeedRetry()) {
                wrappered.cancel();
                this.mqProducerImpl.removeWrapperFuture(correlationID);
                if (wrappered.getCallback() != null) {
                    if (asyncHookService != null) {
                        asyncHookService.execute(() -> {
                            wrappered.getCallback().onFailure(new CraneClientException("Retry time got max"));
                        });
                    } else {
                        wrappered.getCallback().onFailure(new CraneClientException("Retry time got max"));
                    }
                }
            } else { // 重试，但之前延时队列里还有之前的任务，需要屏蔽
                wrappered.increaseRetryTime();
                wrappered.setStartTime(System.currentTimeMillis());
                log.info("Request {} retry because Failure response", correlationID);
                this.mqProducerImpl.asyncSend(wrappered);
            }

        } else { // 成功的响应
            wrappered.setResponse(remoteCommand);
            this.mqProducerImpl.removeWrapperFuture(correlationID);
            if (wrappered.getCallback() != null) {
                asyncHookService.execute(() -> {
                    SendResult result = new SendResult(SendResultType.SEDN_OK, correlationID);
                    wrappered.getCallback().onSuccess(result);
                });
            }
            log.info("Request {} get Success response", correlationID);
        }
        if (hook != null) {
            if (asyncHookService != null) {
                asyncHookService.execute(hook::afterMessage);
            } else {
                hook.afterMessage();
            }
        }
    }
    public void processUpdateTopicResponse(RemoteCommand remoteCommand, ExecutorService asyncHookService) {
        RpcType rpcType = remoteCommand.getHeader().getRpcType();
        int responseCode = remoteCommand.getHeader().getStatus();
        String correlationID = remoteCommand.getHeader().getCorrelationId();
        WrapperFutureCommand wrappered = this.mqProducerImpl.getWrapperFuture(correlationID);
        if (wrappered == null) {
            log.error("Update topic request has been deleted wrongly");
            return;
        }
        if (rpcType == RpcType.SYNC) {
            wrappered.setResponse(remoteCommand);
        } else {
            InnerCallback innerCallback = (InnerCallback) wrappered.getCallback();
            if (asyncHookService != null) {
                asyncHookService.execute(() -> {
                    log.info("Async execute update topic callback");
                    innerCallback.onResponse(remoteCommand);
                });
            } else {
                innerCallback.onResponse(remoteCommand);
            }
        }
    }

    public void processCreateTopicResponse(RemoteCommand remoteCommand, ExecutorService asyncHookService) {
        RpcType rpcType = remoteCommand.getHeader().getRpcType();
        int responseCode = remoteCommand.getHeader().getStatus();
        String correlationID = remoteCommand.getHeader().getCorrelationId();
        WrapperFutureCommand wrappered = this.mqProducerImpl.getWrapperFuture(correlationID);
        if (wrappered == null) {
            log.error("Create topic request has been deleted wrongly");
            return;
        }
        if (rpcType == RpcType.SYNC) {
            wrappered.setResponse(remoteCommand);
        }
    }
}
