package com.github.xjtuwsn.cranemq.client.processor;

import com.github.xjtuwsn.cranemq.client.remote.ClientInstance;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
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
}
