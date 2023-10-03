package com.github.xjtuwsn.cranemq.broker.processors;

import com.github.xjtuwsn.cranemq.broker.BrokerController;
import com.github.xjtuwsn.cranemq.common.command.Header;
import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.command.payloads.MQCreateTopicRequest;
import com.github.xjtuwsn.cranemq.common.command.payloads.MQCreateTopicResponse;
import com.github.xjtuwsn.cranemq.common.command.payloads.MQProduceRequest;
import com.github.xjtuwsn.cranemq.common.command.payloads.MQProduceResponse;
import com.github.xjtuwsn.cranemq.common.command.types.ResponseType;
import com.github.xjtuwsn.cranemq.common.command.types.RpcType;
import com.github.xjtuwsn.cranemq.common.constant.MQConstant;
import com.github.xjtuwsn.cranemq.common.entity.Message;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.github.xjtuwsn.cranemq.common.route.BrokerData;
import com.github.xjtuwsn.cranemq.common.route.QueueData;
import com.github.xjtuwsn.cranemq.common.route.TopicRouteInfo;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @project:cranemq
 * @file:BrokerProcessor
 * @author:wsn
 * @create:2023/10/02-16:07
 */
public class BrokerProcessor {

    private static final Logger log = LoggerFactory.getLogger(BrokerProcessor.class);
    private BrokerController brokerController;

    public BrokerProcessor() {
    }

    public BrokerProcessor(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void processProduceMessage(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {
        System.out.println(remoteCommand);
        Header header = remoteCommand.getHeader();
        MQProduceRequest messageProduceRequest = (MQProduceRequest) remoteCommand.getPayLoad();
        this.brokerController.getMessageStoreCenter().putMessage(remoteCommand);
        /**
         * 存储消息
         * 等等操作
         */
        if (header.getRpcType() == RpcType.ONE_WAY) {
            return;
        }
        Header responseHeader = new Header(ResponseType.PRODUCE_MESSAGE_RESPONSE, header.getRpcType(),
                header.getCorrelationId());
        PayLoad responsePayload = new  MQProduceResponse("");
        RemoteCommand response = new RemoteCommand(responseHeader, responsePayload);
        log.info("Broker receve produce message: {}", messageProduceRequest);
        ctx.writeAndFlush(response);
    }
    public void processCreateTopic(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {
        log.info("------------Create Topic Request----------------");
        MQCreateTopicRequest mqCreateTopicRequest = (MQCreateTopicRequest) remoteCommand.getPayLoad();

        /**
         * 创建主题等等
         */

        Header responseHeader = new Header(ResponseType.CREATE_TOPIC_RESPONSE,
                remoteCommand.getHeader().getRpcType(), remoteCommand.getHeader().getCorrelationId());
        String topic = mqCreateTopicRequest.getTopic(), brokerName = "brokder1";
        BrokerData brokerData = new BrokerData(brokerName);
        QueueData queueData = new QueueData(brokerName, 4, 4);
        brokerData.putQueueData(MQConstant.MASTER_ID, queueData);
        brokerData.putAddress(MQConstant.MASTER_ID, "127.0.0.1:9999");
        List<BrokerData> list1 = new ArrayList<>();
        list1.add(brokerData);
        TopicRouteInfo info = new TopicRouteInfo(topic, list1);
        PayLoad payLoad = new MQCreateTopicResponse(info);
        RemoteCommand response = new RemoteCommand(responseHeader, payLoad);
        log.info("----------------Create Topic Finished-------------");
        ctx.writeAndFlush(remoteCommand);
    }

    public void processHeartBeat(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {
        System.out.println(remoteCommand);
    }
}
