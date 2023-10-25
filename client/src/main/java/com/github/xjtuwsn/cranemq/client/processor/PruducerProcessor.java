package com.github.xjtuwsn.cranemq.client.processor;

import com.github.xjtuwsn.cranemq.client.remote.ClientInstance;
import com.github.xjtuwsn.cranemq.common.remote.RemoteHook;
import com.github.xjtuwsn.cranemq.client.remote.WrapperFutureCommand;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.command.types.RpcType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

/**
 * @project:cranemq
 * @file:PruducerProcessor
 * @author:wsn
 * @create:2023/09/28-11:14
 */
public class PruducerProcessor extends AbstractClientProcessor {
    private static final Logger log = LoggerFactory.getLogger(PruducerProcessor.class);

    public PruducerProcessor(ClientInstance clientInstance) {
        super(clientInstance);
    }
    @Override
    public void processMessageProduceResopnse(RemoteCommand remoteCommand,
                                              ExecutorService asyncHookService,
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
    public void processCreateTopicResponse(RemoteCommand remoteCommand, ExecutorService asyncHookService) {
        RpcType rpcType = remoteCommand.getHeader().getRpcType();
        int responseCode = remoteCommand.getHeader().getStatus();
        String correlationID = remoteCommand.getHeader().getCorrelationId();
        WrapperFutureCommand wrappered = this.clientInstance.getWrapperFuture(correlationID);
        if (wrappered == null) {
            log.error("Create topic request has been deleted wrongly");
            return;
        }
        if (rpcType == RpcType.SYNC) {
            wrappered.setResponse(remoteCommand);
        }
    }
}
