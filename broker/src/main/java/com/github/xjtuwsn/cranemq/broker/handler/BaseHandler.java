package com.github.xjtuwsn.cranemq.broker.handler;

import com.github.xjtuwsn.cranemq.broker.enums.HandlerType;
import com.github.xjtuwsn.cranemq.broker.processors.BrokerProcessor;
import com.github.xjtuwsn.cranemq.broker.remote.RemoteServer;
import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.command.payloads.MQCreateTopicRequest;
import com.github.xjtuwsn.cranemq.common.command.payloads.MQCreateTopicResponse;
import com.github.xjtuwsn.cranemq.common.command.types.ResponseCode;
import com.github.xjtuwsn.cranemq.common.command.types.Type;
import com.github.xjtuwsn.cranemq.common.constant.MQConstant;
import com.github.xjtuwsn.cranemq.common.route.BrokerData;
import com.github.xjtuwsn.cranemq.common.route.QueueData;
import com.github.xjtuwsn.cranemq.common.route.TopicRouteInfo;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import com.github.xjtuwsn.cranemq.common.command.Header;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.command.types.RequestType;
import com.github.xjtuwsn.cranemq.common.command.payloads.MQProduceRequest;
import com.github.xjtuwsn.cranemq.common.command.types.ResponseType;
import com.github.xjtuwsn.cranemq.common.entity.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.concurrent.ExecutorService;

/**
 * @project:cranemq
 * @file:BaseHandler
 * @author:wsn
 * @create:2023/09/26-21:36
 */
public class BaseHandler extends SimpleChannelInboundHandler<RemoteCommand> {
    private static final Logger log = LoggerFactory.getLogger(BaseHandler.class);
    private RemoteServer remoteServer;
    private BrokerProcessor brokerProcessor;
    public BaseHandler(RemoteServer remoteServer) {
        this.remoteServer = remoteServer;
        this.brokerProcessor = new BrokerProcessor(this.remoteServer.getController());
    }
    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, RemoteCommand request) throws Exception {
        if (request == null) {
            log.error("Receve null request from client");
            return;
        }
        RequestType type = (RequestType) request.getHeader().getCommandType();
        switch (type) {
            case MESSAGE_PRODUCE_REQUEST:
                doProducerMessageProcess(channelHandlerContext, request);
                break;
            case CREATE_TOPIC_REQUEST:
                doCreateTopicProcess(channelHandlerContext, request);
                break;
            case HEARTBEAT:
                doHeartBeatProcess(channelHandlerContext, request);
            default:
                break;
        }
    }
    private void doProducerMessageProcess(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {
        ExecutorService pool = this.remoteServer.getThreadPool(HandlerType.PRODUCER_REQUEST);
        if (pool != null) {
            pool.execute(() -> {
                this.brokerProcessor.processProduceMessage(ctx, remoteCommand);
            });
        }
    }
    private void doCreateTopicProcess(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {
        ExecutorService pool = this.remoteServer.getThreadPool(HandlerType.CREATE_TOPIC);
        if (pool != null) {
            pool.execute(() -> {
                this.brokerProcessor.processCreateTopic(ctx, remoteCommand);
            });
        }
    }
    private void doHeartBeatProcess(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {
        ExecutorService pool = this.remoteServer.getThreadPool(HandlerType.HEARTBEAT_REQUEST);
        if (pool != null) {
            pool.execute(() -> {
                this.brokerProcessor.processHeartBeat(ctx, remoteCommand);
            });
        }
    }
}
