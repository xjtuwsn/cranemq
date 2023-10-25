package com.github.xjtuwsn.cranemq.client.processor;

import com.github.xjtuwsn.cranemq.client.remote.WrapperFutureCommand;
import com.github.xjtuwsn.cranemq.client.producer.result.SendResult;
import com.github.xjtuwsn.cranemq.client.producer.result.SendResultType;
import com.github.xjtuwsn.cranemq.client.remote.ClientInstance;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.command.types.ResponseCode;
import com.github.xjtuwsn.cranemq.common.command.types.RpcType;
import com.github.xjtuwsn.cranemq.common.exception.CraneClientException;
import com.github.xjtuwsn.cranemq.common.remote.processor.BaseProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

/**
 * @project:cranemq
 * @file:AbstractClientProcessor
 * @author:wsn
 * @create:2023/10/07-19:29
 */
public abstract class AbstractClientProcessor implements BaseProcessor {

    private static final Logger log = LoggerFactory.getLogger(AbstractClientProcessor.class);

    protected ClientInstance clientInstance;

    public AbstractClientProcessor(ClientInstance clientInstance) {
        this.clientInstance = clientInstance;
    }

    protected WrapperFutureCommand parseResponseWithRetry(RemoteCommand remoteCommand,
                                          ExecutorService asyncHookService) {
        RpcType rpcType = remoteCommand.getHeader().getRpcType();
        int responseCode = remoteCommand.getHeader().getStatus();
        String correlationID = remoteCommand.getHeader().getCorrelationId();
        WrapperFutureCommand wrappered = this.clientInstance.getWrapperFuture(correlationID);
        if (wrappered == null) {
            log.warn("Request {} has been removed before", correlationID);
            return null;
        }
        if (responseCode != ResponseCode.SUCCESS) {
            log.warn("Request {} has occured a mistake, reason is {}", correlationID, responseCode);
            if (wrappered.isDone()) {
                this.clientInstance.removeWrapperFuture(correlationID);
                return wrappered;
            }
            if (!wrappered.isNeedRetry()) {
                wrappered.cancel();
                this.clientInstance.removeWrapperFuture(correlationID);
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
                this.clientInstance.sendMessageAsync(wrappered);
            }

        } else { // 成功的响应
            wrappered.setResponse(remoteCommand);
            this.clientInstance.removeWrapperFuture(correlationID);
            if (wrappered.getCallback() != null) {
                asyncHookService.execute(() -> {
                    SendResult result = new SendResult(SendResultType.SEND_OK, correlationID);
                    wrappered.getCallback().onSuccess(result);
                });
            }
            log.info("Request {} get Success response", correlationID);
        }
        return wrappered;
    }

}
