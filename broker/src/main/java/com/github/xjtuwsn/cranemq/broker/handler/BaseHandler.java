package com.github.xjtuwsn.cranemq.broker.handler;

import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.command.payloads.MQCreateTopicRequest;
import com.github.xjtuwsn.cranemq.common.command.payloads.MQCreateTopicResponse;
import com.github.xjtuwsn.cranemq.common.command.types.ResponseCode;
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
import java.util.Random;

/**
 * @project:cranemq
 * @file:BaseHandler
 * @author:wsn
 * @create:2023/09/26-21:36
 */
public class BaseHandler extends SimpleChannelInboundHandler<RemoteCommand> {
    private static final Logger log = LoggerFactory.getLogger(BaseHandler.class);
    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, RemoteCommand request) throws Exception {
        if (request.getHeader().getCommandType() == RequestType.MESSAGE_PRODUCE_REQUEST) {
            MQProduceRequest messageProduceRequest = (MQProduceRequest) request.getPayLoad();
            log.info("Broker receve produce message: {}", messageProduceRequest);
            Header header = new Header(ResponseType.PRODUCE_MESSAGE_RESPONSE,
                    request.getHeader().getRpcType(), request.getHeader().getCorrelationId());
            int right = 10000;
            int rand = new Random().nextInt(right);
//            if (rand > 99990) header.onFailure(ResponseCode.DEFAULT_ERROR);
            RemoteCommand command = new RemoteCommand(header, new MQProduceRequest(new Message()));
            channelHandlerContext.writeAndFlush(command);
        } else if (request.getHeader().getCommandType() == RequestType.CREATE_TOPIC_REQUEST) {
            log.info("------------Create Topic Request----------------");
            MQCreateTopicRequest mqCreateTopicRequest = (MQCreateTopicRequest) request.getPayLoad();
            Header header = new Header(ResponseType.CREATE_TOPIC_RESPONSE,
                    request.getHeader().getRpcType(), request.getHeader().getCorrelationId());

            String topic = mqCreateTopicRequest.getTopic(), brokerName = "brokder1";
            BrokerData brokerData = new BrokerData(brokerName);
            QueueData queueData = new QueueData(brokerName, 4, 4);
            brokerData.putQueueData(MQConstant.MASTER_ID, queueData);
            brokerData.putAddress(MQConstant.MASTER_ID, "127.0.0.1:9999");
            List<BrokerData> list1 = new ArrayList<>();
            list1.add(brokerData);
            TopicRouteInfo info = new TopicRouteInfo(topic, list1);
            PayLoad payLoad = new MQCreateTopicResponse(info);
            RemoteCommand remoteCommand = new RemoteCommand(header, payLoad);
            log.info("----------------Create Topic Finished-------------");
            channelHandlerContext.writeAndFlush(remoteCommand);
        }
    }
}
