package com.github.xjtuwsn.cranemq.client.processor;

import com.github.xjtuwsn.cranemq.client.hook.InnerCallback;
import com.github.xjtuwsn.cranemq.client.remote.WrapperFutureCommand;
import com.github.xjtuwsn.cranemq.client.remote.ClientInstance;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.command.types.RpcType;
import com.github.xjtuwsn.cranemq.common.remote.processor.BaseProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

/**
 * @project:cranemq
 * @file:CommonProcessor
 * @author:wsn
 * @create:2023/10/07-16:37
 */
public class CommonProcessor implements BaseProcessor {
    private static final Logger log = LoggerFactory.getLogger(CommonProcessor.class);

    private ClientInstance clientInstance;

    public CommonProcessor(ClientInstance clientInstance) {
        this.clientInstance = clientInstance;
    }

    @Override
    public void processUpdateTopicResponse(RemoteCommand remoteCommand, ExecutorService asyncHookService) {
        RpcType rpcType = remoteCommand.getHeader().getRpcType();
        int responseCode = remoteCommand.getHeader().getStatus();
        String correlationID = remoteCommand.getHeader().getCorrelationId();
        WrapperFutureCommand wrappered = this.clientInstance.getWrapperFuture(correlationID);
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
}
