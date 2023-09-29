package com.github.xjtuwsn.cranemq.registry.handler;

import com.github.xjtuwsn.cranemq.common.command.Header;
import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.command.payloads.MQProduceRequest;
import com.github.xjtuwsn.cranemq.common.command.payloads.MQUpdateTopicRequest;
import com.github.xjtuwsn.cranemq.common.command.payloads.MQUpdateTopicResponse;
import com.github.xjtuwsn.cranemq.common.command.types.RequestType;
import com.github.xjtuwsn.cranemq.common.command.types.ResponseType;
import com.github.xjtuwsn.cranemq.common.entity.Message;
import com.github.xjtuwsn.cranemq.common.route.BrokerData;
import com.github.xjtuwsn.cranemq.common.route.QueueData;
import com.github.xjtuwsn.cranemq.common.route.TopicRouteInfo;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @project:cranemq
 * @file:BaseHandler
 * @author:wsn
 * @create:2023/09/29-14:39
 */
public class BaseHandler extends SimpleChannelInboundHandler<RemoteCommand> {
    private ConcurrentHashMap<String, TopicRouteInfo> map = new ConcurrentHashMap<>();
    public BaseHandler() {

        String topic = "topic1", brokerName = "brokder1";
        BrokerData brokerData = new BrokerData(brokerName, "127.0.0.1:9999");
        QueueData queueData = new QueueData(brokerName, 4, 4);
        List<BrokerData> list1 = new ArrayList<>();
        List<QueueData> list2 = new ArrayList<>();
        list1.add(brokerData); list2.add(queueData);
        TopicRouteInfo info = new TopicRouteInfo(topic, list1, list2);
        this.map.put(topic, info);
    }
    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, RemoteCommand request) throws Exception {
        if (request.getHeader().getCommandType() == RequestType.UPDATE_TOPIC_REQUEST) {
            MQUpdateTopicRequest messageProduceRequest = (MQUpdateTopicRequest) request.getPayLoad();
            String topic = messageProduceRequest.getTopic();
            Header header = new Header(ResponseType.UPDATE_TOPIC_RESPONSE,
                    request.getHeader().getRpcType(), request.getHeader().getCorrelationId());
            TopicRouteInfo info = this.map.get(topic);
            PayLoad payLoad = new MQUpdateTopicResponse(topic, info);
            RemoteCommand command = new RemoteCommand(header, payLoad);
            channelHandlerContext.writeAndFlush(command);
        }
    }
}
