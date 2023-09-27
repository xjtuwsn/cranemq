package com.github.xjtuwsn.cranemq.client.remote.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.xjtuwsn.cranemq.client.hook.RemoteHook;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.command.types.ResponseType;
import com.github.xjtuwsn.cranemq.common.command.types.RpcType;
import com.github.xjtuwsn.cranemq.common.command.types.Type;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @project:cranemq
 * @file:BaseProducerHandler
 * @author:wsn
 * @create:2023/09/27-15:13
 */
public class BaseProducerHandler extends SimpleChannelInboundHandler<RemoteCommand> {
    private static final Logger log = LoggerFactory.getLogger(BaseProducerHandler.class);
    private RemoteHook hook;

    private ExecutorService asyncCallBackService;
    private int coreSize = 3;
    private int maxSize = 5;
    public BaseProducerHandler(RemoteHook hook) {
        this.hook = hook;
        this.asyncCallBackService = new ThreadPoolExecutor(coreSize,
                maxSize,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(100),
                new ThreadFactory() {
                    AtomicInteger count = new AtomicInteger(0);
                    @Override
                    public Thread newThread(Runnable r) {
                        int andIncrement = count.getAndIncrement();
                        return new Thread(r, "CallBack handler thread, no." + andIncrement);
                    }
                },
                new ThreadPoolExecutor.AbortPolicy());
    }
    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, RemoteCommand remoteCommand) throws Exception {
        System.out.println(23232);
        if (remoteCommand == null) {
            log.error("Receive null message from broker");
            return;
        }
        Type type = remoteCommand.getHeader().getCommandType();
        if (!(type instanceof ResponseType)) {
            log.error("Receive wrong type response, {}", type);
            return;
        }
        RpcType rpcType = remoteCommand.getHeader().getRpcType();
        if (rpcType == RpcType.ASYNC) {
            if (hook != null) {
                asyncCallBackService.execute(() -> {
                    hook.afterMessage();
                    log.info("Finish handle after message hook");
                });
            }

        }
    }
}
