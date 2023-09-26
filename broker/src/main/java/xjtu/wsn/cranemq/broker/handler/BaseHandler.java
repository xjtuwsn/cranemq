package xjtu.wsn.cranemq.broker.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import xjtu.wsn.cranemq.common.request.BaseRequest;
import xjtu.wsn.cranemq.common.request.MessageProduceRequest;
import xjtu.wsn.cranemq.common.request.RequestType;

/**
 * @project:cranemq
 * @file:BaseHandler
 * @author:wsn
 * @create:2023/09/26-21:36
 */
public class BaseHandler extends SimpleChannelInboundHandler<BaseRequest> {
    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, BaseRequest request) throws Exception {
        if (request.getDataType() == RequestType.MESSAGE_PRODUCE) {
            MessageProduceRequest messageProduceRequest = (MessageProduceRequest) request.getData();
        }
    }
}
