package com.github.xjtuwsn.cranemq.client.remote.registry;

import com.github.xjtuwsn.cranemq.client.WrapperFutureCommand;
import com.github.xjtuwsn.cranemq.client.hook.InnerCallback;
import com.github.xjtuwsn.cranemq.client.producer.result.SendResult;
import com.github.xjtuwsn.cranemq.client.producer.result.SendResultType;
import com.github.xjtuwsn.cranemq.client.remote.ClientInstance;
import com.github.xjtuwsn.cranemq.common.command.FutureCommand;
import com.github.xjtuwsn.cranemq.common.command.Header;
import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.command.payloads.req.MQQueryTopicRequest;
import com.github.xjtuwsn.cranemq.common.command.payloads.resp.MQQueryTopicResponse;
import com.github.xjtuwsn.cranemq.common.command.types.RequestType;
import com.github.xjtuwsn.cranemq.common.command.types.RpcType;
import com.github.xjtuwsn.cranemq.common.remote.RegistryCallback;
import com.github.xjtuwsn.cranemq.common.remote.RemoteHook;
import com.github.xjtuwsn.cranemq.common.remote.RemoteRegistry;
import com.github.xjtuwsn.cranemq.common.route.TopicRouteInfo;
import com.github.xjtuwsn.cranemq.common.utils.TopicUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @project:cranemq
 * @file:SimpleRegistry
 * @author:wsn
 * @create:2023/10/15-15:33
 */
public class SimpleRegistry implements RemoteRegistry {
    private static final Logger log = LoggerFactory.getLogger(SimpleRegistry.class);

    private ClientInstance clientInstance;

    public SimpleRegistry(ClientInstance clientInstance) {
        this.clientInstance = clientInstance;
    }
    @Override
    public TopicRouteInfo fetchRouteInfo(String topic) {
        Header header = new Header(RequestType.QUERY_TOPIC_REQUEST,
                RpcType.SYNC, TopicUtil.generateUniqueID());
        PayLoad payLoad = new MQQueryTopicRequest(topic);
        RemoteCommand remoteCommand = new RemoteCommand(header, payLoad);
        FutureCommand futureCommand = new FutureCommand();
        futureCommand.setRequest(remoteCommand);
        WrapperFutureCommand wrappered = new WrapperFutureCommand(futureCommand, topic, -1, null);
        wrappered.setToRegistery(true);
        SendResult result = this.clientInstance.sendMessageSync(wrappered, false);

        if (result.getResultType() == SendResultType.SERVER_ERROR || result.getTopicRouteInfo() == null) {
            log.error("Topic {} cannot find correct broker", topic);
            return null;
        }

        return result.getTopicRouteInfo();
    }

    @Override
    public void fetchRouteInfo(String topic, RegistryCallback callback) {
        Header header = new Header(RequestType.QUERY_TOPIC_REQUEST,
                RpcType.ASYNC, TopicUtil.generateUniqueID());
        PayLoad payLoad = new MQQueryTopicRequest(topic);
        RemoteCommand remoteCommand = new RemoteCommand(header, payLoad);
        FutureCommand futureCommand = new FutureCommand();
        futureCommand.setRequest(remoteCommand);
        WrapperFutureCommand wrappered = new WrapperFutureCommand(futureCommand, topic, -1, new InnerCallback() {
            @Override
            public void onResponse(RemoteCommand remoteCommand) {
                MQQueryTopicResponse response = (MQQueryTopicResponse) remoteCommand.getPayLoad();
                TopicRouteInfo info = response.getRouteInfo();
                if (callback != null) {
                    callback.onRouteInfo(info);
                }
            }
        });
        wrappered.setToRegistery(true);
        this.clientInstance.sendMessageAsync(wrappered);
    }

    @Override
    public void uploadRouteInfo(TopicRouteInfo info) {

    }

    @Override
    public void append() {

    }

    @Override
    public void start() {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public void registerHook(RemoteHook hook) {

    }
}
