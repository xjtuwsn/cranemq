package com.github.xjtuwsn.cranemq.registry.processors;

import com.github.xjtuwsn.cranemq.common.command.Header;
import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.command.payloads.req.MQQueryTopicRequest;
import com.github.xjtuwsn.cranemq.common.command.payloads.req.MQUpdateTopicRequest;
import com.github.xjtuwsn.cranemq.common.command.payloads.resp.MQQueryTopicResponse;
import com.github.xjtuwsn.cranemq.common.command.types.ResponseType;
import com.github.xjtuwsn.cranemq.common.remote.enums.ConnectionEventType;
import com.github.xjtuwsn.cranemq.common.remote.event.ConnectionEvent;
import com.github.xjtuwsn.cranemq.common.remote.processor.BaseProcessor;
import com.github.xjtuwsn.cranemq.common.route.BrokerData;
import com.github.xjtuwsn.cranemq.common.route.QueueData;
import com.github.xjtuwsn.cranemq.common.route.TopicRouteInfo;
import com.github.xjtuwsn.cranemq.registry.RegistryController;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @project:cranemq
 * @file:RegistryProcessor
 * @author:wsn
 * @create:2023/10/15-20:06
 */
public class RegistryProcessor implements BaseProcessor {
    private static final Logger log = LoggerFactory.getLogger(RegistryProcessor.class);
    private RegistryController registryController;

    public RegistryProcessor(RegistryController registryController) {
        this.registryController = registryController;
    }

    @Override
    public void processQueryRouteRequest(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {
        Header header = remoteCommand.getHeader();
        MQQueryTopicRequest mqQueryTopicRequest = (MQQueryTopicRequest) remoteCommand.getPayLoad();
        String topic = mqQueryTopicRequest.getTopic();
        TopicRouteInfo topicRouteInfo = this.registryController.getTopicInfoHolder().getTopicRouteInfo(topic);
        Header responseHeader = new Header(ResponseType.QUERY_TOPIC_RESPONSE, header.getRpcType(), header.getCorrelationId());
        PayLoad payLoad = new MQQueryTopicResponse(topic, topicRouteInfo);
        RemoteCommand response = new RemoteCommand(responseHeader, payLoad);
        ctx.writeAndFlush(response);
    }

    @Override
    public void processUpdateRequest(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {
        Header header = remoteCommand.getHeader();
        MQUpdateTopicRequest mqUpdateTopicRequest = (MQUpdateTopicRequest) remoteCommand.getPayLoad();
        String brokerName = mqUpdateTopicRequest.getBrokerName();
        String address = mqUpdateTopicRequest.getAddress();
        int id = mqUpdateTopicRequest.getId();
        Map<String, QueueData> queueDatas = mqUpdateTopicRequest.getQueueDatas();
        this.registryController.getTopicInfoHolder().update(brokerName, id, queueDatas, address);
        this.registryController.getRemoteServer().publishEvent(
                new ConnectionEvent(ConnectionEventType.BROKER_HEARTBEAT, ctx.channel(), brokerName, id));
    }
}
