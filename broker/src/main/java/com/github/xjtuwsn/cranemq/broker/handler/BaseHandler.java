package com.github.xjtuwsn.cranemq.broker.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import com.github.xjtuwsn.cranemq.common.command.Header;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.command.types.RequestType;
import com.github.xjtuwsn.cranemq.common.command.payloads.MQProduceRequest;
import com.github.xjtuwsn.cranemq.common.command.types.ResponseType;
import com.github.xjtuwsn.cranemq.common.entity.Message;

/**
 * @project:cranemq
 * @file:BaseHandler
 * @author:wsn
 * @create:2023/09/26-21:36
 */
public class BaseHandler extends SimpleChannelInboundHandler<RemoteCommand> {
    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, RemoteCommand request) throws Exception {
        if (request.getHeader().getCommandType() == RequestType.MESSAGE_PRODUCE_REQUEST) {
            System.out.println(request);
            MQProduceRequest messageProduceRequest = (MQProduceRequest) request.getPayLoad();
            System.out.println(messageProduceRequest);
            Header header = new Header(ResponseType.RESPONSE_SUCCESS,
                    request.getHeader().getRpcType(), request.getHeader().getCorrelationId());
            RemoteCommand command = new RemoteCommand(header, new MQProduceRequest(new Message()));
            channelHandlerContext.writeAndFlush(command);
        }
    }
}
