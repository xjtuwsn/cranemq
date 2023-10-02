package com.github.xjtuwsn.cranemq.client.remote.handler;

import com.github.xjtuwsn.cranemq.client.processor.PruducerProcessor;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.xjtuwsn.cranemq.common.net.RemoteHook;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.command.types.ResponseType;
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
    private PruducerProcessor processor;
    private int coreSize = 8;
    private int maxSize = 16;
    public BaseProducerHandler(RemoteHook hook, PruducerProcessor processor) {
        this.hook = hook;
        this.processor = processor;
        this.asyncCallBackService = new ThreadPoolExecutor(coreSize,
                maxSize,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(1000),
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
    // TODO 响应解析
    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, RemoteCommand remoteCommand) throws Exception {
        if (remoteCommand == null) {
            log.error("Receive null message from broker");
            return;
        }

        Type type = remoteCommand.getHeader().getCommandType();
        if (!(type instanceof ResponseType)) {
            log.error("Receive wrong type response, {}", type);
            return;
        }
        switch ((ResponseType) type) {
            case PRODUCE_MESSAGE_RESPONSE:
                doProduceProcessor(remoteCommand);
                break;
            case QUERY_BROKER_RESPONSE:
                break;
            case UPDATE_TOPIC_RESPONSE:
                doUpdateTopicProcessor(remoteCommand);
                break;
            case CREATE_TOPIC_RESPONSE:
                doCreateTopicProcessor(remoteCommand);
                break;
            default:
                break;
        }

    }
    private void doProduceProcessor(RemoteCommand remoteCommand) {
        processor.processMessageProduceResopnse(remoteCommand, this.asyncCallBackService, this.hook);
    }
    private void doUpdateTopicProcessor(RemoteCommand remoteCommand) {
        processor.processUpdateTopicResponse(remoteCommand, this.asyncCallBackService);
    }
    private void doCreateTopicProcessor(RemoteCommand remoteCommand) {
        processor.processCreateTopicResponse(remoteCommand, this.asyncCallBackService);
    }
    private void doQueryBrokerProcessor(RemoteCommand remoteCommand) {

    }
}
