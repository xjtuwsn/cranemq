package com.github.xjtuwsn.cranemq.broker.processors;

import com.github.xjtuwsn.cranemq.broker.BrokerController;
import com.github.xjtuwsn.cranemq.common.command.payloads.req.*;
import com.github.xjtuwsn.cranemq.common.command.payloads.resp.*;
import com.github.xjtuwsn.cranemq.common.command.types.*;
import com.github.xjtuwsn.cranemq.common.entity.Message;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.github.xjtuwsn.cranemq.common.remote.enums.ConnectionEventType;
import com.github.xjtuwsn.cranemq.common.remote.event.ConnectionEvent;
import com.github.xjtuwsn.cranemq.common.remote.RemoteServer;
import com.github.xjtuwsn.cranemq.broker.store.comm.StoreInnerMessage;
import com.github.xjtuwsn.cranemq.broker.store.comm.PutMessageResponse;
import com.github.xjtuwsn.cranemq.broker.store.comm.StoreResponseType;
import com.github.xjtuwsn.cranemq.common.command.Header;
import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
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
import java.util.Map;
import java.util.Set;

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
        if (header.getCommandType() == RequestType.MESSAGE_PRODUCE_REQUEST) {
            MQProduceRequest messageProduceRequest = (MQProduceRequest) remoteCommand.getPayLoad();
            StoreInnerMessage storeInnerMessage = new StoreInnerMessage(messageProduceRequest.getMessage(),
                    messageProduceRequest.getWriteQueue(), header.getCorrelationId());
            log.info("Broker receve produce message: {}", messageProduceRequest);
            long start = System.nanoTime();
            PutMessageResponse putResp = this.brokerController.getMessageStoreCenter().putMessage(storeInnerMessage);
            long end = System.nanoTime();


            double cost = (end - start) / 1e6;
            log.warn("Cost {} ms", cost);

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
        } else {
            // todo 批量消息
            MQBachProduceRequest mqBachProduceRequest = (MQBachProduceRequest) remoteCommand.getPayLoad();
            List<Message> messages = mqBachProduceRequest.getMessages();
            MessageQueue writeQueue = mqBachProduceRequest.getWriteQueue();

            List<StoreInnerMessage> list = new ArrayList<>();

            for (Message message : messages) {
                StoreInnerMessage storeInnerMessage = new StoreInnerMessage(message,
                        writeQueue, header.getCorrelationId());
                list.add(storeInnerMessage);
            }

            PutMessageResponse putResp = this.brokerController.getMessageStoreCenter().putMessage(list);
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
        if (heartBeatRequest.getProducerGroup() != null && heartBeatRequest.getProducerGroup().size() != 0) {
            this.remoteServer.publishEvent(new ConnectionEvent(ConnectionEventType.PRODUCER_HEARTBEAT, ctx.channel(),
                    heartBeatRequest));
        } else {
            this.remoteServer.publishEvent(new ConnectionEvent(ConnectionEventType.CONSUMER_HEARTBEAT, ctx.channel(),
                    heartBeatRequest));
        }
//        System.out.println(remoteCommand);
    }
    // TODO 测试读取的对不对
    @Override
    public void processSimplePull(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {
        Header header = remoteCommand.getHeader();
        MQSimplePullRequest pullRequest = (MQSimplePullRequest) remoteCommand.getPayLoad();

        long start = System.nanoTime();
        MQSimplePullResponse response = brokerController.getMessageStoreCenter().simplePullMessage(pullRequest);
        long end = System.nanoTime();
        double cost = (end - start) / 1e6;
        log.warn("Pull {} message cost {} ms", pullRequest.getLength(), cost);

        Header responseHeader = new Header(ResponseType.SIMPLE_PULL_RESPONSE, header.getRpcType(),
                header.getCorrelationId());
        if (response.getResultType() != AcquireResultType.DONE) {
            responseHeader.onFailure(ResponseCode.SERVER_ERROR);
        }
        RemoteCommand resopnse = new RemoteCommand(responseHeader, response);
        ctx.writeAndFlush(resopnse);
    }

    @Override
    public void processPullRequest(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {
        MQPullMessageRequest mqPullMessageRequest = (MQPullMessageRequest) remoteCommand.getPayLoad();
        Header header = remoteCommand.getHeader();
        this.brokerController.getHoldRequestService().tryHoldRequest(mqPullMessageRequest, ctx.channel(),
                header.getCorrelationId());
    }

    @Override
    public void processQueryRequest(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {
        Header header = remoteCommand.getHeader();
        MQReblanceQueryRequest mqReblanceQueryRequest = (MQReblanceQueryRequest) remoteCommand.getPayLoad();
        String group = mqReblanceQueryRequest.getGroup();
        Set<String> topics = mqReblanceQueryRequest.getTopics();
        Set<String> groupClients = this.brokerController.getConsumerGroupManager().getGroupClients(group);
        Map<MessageQueue, Long> allGroupOffset = this.brokerController.getOffsetManager()
                        .getAllGroupOffset(group, topics);

        header.setCommandType(ResponseType.QUERY_BROKER_RESPONSE);
        MQRebalanceQueryResponse mqRebalanceQueryResponse = new MQRebalanceQueryResponse(group, groupClients,
                allGroupOffset);
        RemoteCommand response = new RemoteCommand(header, mqRebalanceQueryResponse);
        ctx.writeAndFlush(response);

    }

    @Override
    public void processRecordOffsetRequest(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {

        MQRecordOffsetRequest offsetRequest = (MQRecordOffsetRequest) remoteCommand.getPayLoad();

        Map<MessageQueue, Long> offsets = offsetRequest.getOffsets();

        String group = offsetRequest.getGroup();

        this.brokerController.getOffsetManager().updateOffsets(offsets, group);
    }

    @Override
    public void processLockRequest(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {
        Header header = remoteCommand.getHeader();
        MQLockRequest mqLockRequest = (MQLockRequest) remoteCommand.getPayLoad();
        String group = mqLockRequest.getGroup(), clientId = mqLockRequest.getClientId();
        MessageQueue messageQueue = mqLockRequest.getMessageQueue();
        boolean res = false;
        switch (mqLockRequest.getLockType()) {
            case APPLY:
                res = brokerController.getClientLockMananger().applyLock(group, messageQueue, clientId);
                break;
            case RENEW:
                res = brokerController.getClientLockMananger().renewLock(group, messageQueue, clientId);
                break;
            case RELEASE:
                res = brokerController.getClientLockMananger().releaseLock(group, messageQueue, clientId);
            default:
                break;
        }

        Header responseHeader = new Header(ResponseType.LOCK_RESPONSE, header.getRpcType(), header.getCorrelationId());
        PayLoad mqLockRespnse = new MQLockRespnse(res);

        RemoteCommand response = new RemoteCommand(responseHeader, mqLockRespnse);

        ctx.writeAndFlush(response);
    }
}
