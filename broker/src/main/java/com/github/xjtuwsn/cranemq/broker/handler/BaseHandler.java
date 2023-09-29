package com.github.xjtuwsn.cranemq.broker.handler;

import com.github.xjtuwsn.cranemq.common.command.types.ResponseCode;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import com.github.xjtuwsn.cranemq.common.command.Header;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.command.types.RequestType;
import com.github.xjtuwsn.cranemq.common.command.payloads.MQProduceRequest;
import com.github.xjtuwsn.cranemq.common.command.types.ResponseType;
import com.github.xjtuwsn.cranemq.common.entity.Message;

import java.util.Random;

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
            Header header = new Header(ResponseType.PRODUCE_MESSAGE_RESPONSE,
                    request.getHeader().getRpcType(), request.getHeader().getCorrelationId());
            int right = 10000;
            int rand = new Random().nextInt(right);
//            if (rand > 99990) header.onFailure(ResponseCode.DEFAULT_ERROR);
            RemoteCommand command = new RemoteCommand(header, new MQProduceRequest(new Message()));
            channelHandlerContext.writeAndFlush(command);
        }
    }
}
