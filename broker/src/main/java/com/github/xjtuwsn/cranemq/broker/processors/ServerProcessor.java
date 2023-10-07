package com.github.xjtuwsn.cranemq.broker.processors;

import com.github.xjtuwsn.cranemq.broker.BrokerController;
import com.github.xjtuwsn.cranemq.common.command.payloads.req.MQSimplePullRequest;
import com.github.xjtuwsn.cranemq.common.command.payloads.resp.MQSimplePullResponse;
import com.github.xjtuwsn.cranemq.common.remote.enums.ConnectionEventType;
import com.github.xjtuwsn.cranemq.common.remote.event.ConnectionEvent;
import com.github.xjtuwsn.cranemq.common.remote.RemoteServer;
import com.github.xjtuwsn.cranemq.broker.store.comm.StoreInnerMessage;
import com.github.xjtuwsn.cranemq.broker.store.comm.PutMessageResponse;
import com.github.xjtuwsn.cranemq.broker.store.comm.StoreResponseType;
import com.github.xjtuwsn.cranemq.common.command.Header;
import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.command.payloads.req.MQCreateTopicRequest;
import com.github.xjtuwsn.cranemq.common.command.payloads.req.MQHeartBeatRequest;
import com.github.xjtuwsn.cranemq.common.command.payloads.req.MQProduceRequest;
import com.github.xjtuwsn.cranemq.common.command.payloads.resp.MQCreateTopicResponse;
import com.github.xjtuwsn.cranemq.common.command.payloads.resp.MQProduceResponse;
import com.github.xjtuwsn.cranemq.common.command.types.ResponseCode;
import com.github.xjtuwsn.cranemq.common.command.types.ResponseType;
import com.github.xjtuwsn.cranemq.common.command.types.RpcType;
import com.github.xjtuwsn.cranemq.common.constant.MQConstant;
import com.github.xjtuwsn.cranemq.common.remote.processor.BaseProcessor;
import com.github.xjtuwsn.cranemq.common.route.BrokerData;
import com.github.xjtuwsn.cranemq.common.route.QueueData;
import com.github.xjtuwsn.cranemq.common.route.TopicRouteInfo;
import com.github.xjtuwsn.cranemq.common.utils.NetworkUtil;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @project:cranemq
 * @file:BrokerProcessor
 * @author:wsn
 * @create:2023/10/02-16:07
 */
public class ServerProcessor implements BaseProcessor {

    private static final Logger log = LoggerFactory.getLogger(ServerProcessor.class);
    private BrokerController brokerController;
    private RemoteServer remoteServer;

    public ServerProcessor() {
    }

    public ServerProcessor(BrokerController brokerController, RemoteServer remoteServer) {
        this.brokerController = brokerController;
        this.remoteServer = remoteServer;
    }
    @Override
    public void processProduceMessage(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {

        Header header = remoteCommand.getHeader();
        MQProduceRequest messageProduceRequest = (MQProduceRequest) remoteCommand.getPayLoad();
        StoreInnerMessage storeInnerMessage = new StoreInnerMessage(messageProduceRequest.getMessage(),
                messageProduceRequest.getWriteQueue(), header.getCorrelationId());
        log.info("Broker receve produce message: {}", messageProduceRequest);
        long start = System.nanoTime();
        PutMessageResponse putResp = this.brokerController.getMessageStoreCenter().putMessage(storeInnerMessage);
        long end = System.nanoTime();


        double cost = (end - start) / 1e6;
        log.warn("Cost {} ms", cost);
        /**
         * 存储消息
         * 等等操作
         */
        if (header.getRpcType() == RpcType.ONE_WAY) {
            return;
        }
        Header responseHeader = new Header(ResponseType.PRODUCE_MESSAGE_RESPONSE, header.getRpcType(),
                header.getCorrelationId());
        if (putResp.getResponseType() != StoreResponseType.STORE_OK) {
            responseHeader.onFailure(ResponseCode.SERVER_ERROR);
        }
        PayLoad responsePayload = new MQProduceResponse("");
        RemoteCommand response = new RemoteCommand(responseHeader, responsePayload);

        ctx.writeAndFlush(response);
    }
    @Override
    public void processCreateTopic(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {
        log.info("------------Create Topic Request----------------");
        MQCreateTopicRequest mqCreateTopicRequest = (MQCreateTopicRequest) remoteCommand.getPayLoad();

        /**
         * 创建主题等等
         */

        Header responseHeader = new Header(ResponseType.CREATE_TOPIC_RESPONSE,
                remoteCommand.getHeader().getRpcType(), remoteCommand.getHeader().getCorrelationId());
        String topic = mqCreateTopicRequest.getTopic(), brokerName = brokerController.getBrokerConfig().getBrokerName();
        BrokerData brokerData = new BrokerData(brokerName);
        QueueData queueData = brokerController.getMessageStoreCenter().createTopic(mqCreateTopicRequest);

        brokerData.putQueueData(MQConstant.MASTER_ID, queueData);
        String address = NetworkUtil.getLocalAddress() + ":" + brokerController.getBrokerConfig().getPort();
        brokerData.putAddress(MQConstant.MASTER_ID, address);
        List<BrokerData> list1 = new ArrayList<>();
        list1.add(brokerData);
        TopicRouteInfo info = new TopicRouteInfo(topic, list1);

        PayLoad payLoad = new MQCreateTopicResponse(info);
        RemoteCommand response = new RemoteCommand(responseHeader, payLoad);
        log.info("----------------Create Topic Finished-------------");
        ctx.writeAndFlush(response);
    }
    @Override
    public void processHeartBeat(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {
        Header header = remoteCommand.getHeader();
        MQHeartBeatRequest heartBeatRequest = (MQHeartBeatRequest) remoteCommand.getPayLoad();
        if (heartBeatRequest.getProducerGroup() != null) {
            this.remoteServer.publishEvent(new ConnectionEvent(ConnectionEventType.PRODUCER_HEARTBEAT, ctx.channel(),
                    heartBeatRequest));
        } else {

        }
//        System.out.println(remoteCommand);
    }
    // TODO 测试读取的对不对
    @Override
    public void processSimplePull(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {
        Header header = remoteCommand.getHeader();
        MQSimplePullRequest pullRequest = (MQSimplePullRequest) remoteCommand.getPayLoad();
        MQSimplePullResponse response = brokerController.getMessageStoreCenter().simplePullMessage(pullRequest);

    }
}