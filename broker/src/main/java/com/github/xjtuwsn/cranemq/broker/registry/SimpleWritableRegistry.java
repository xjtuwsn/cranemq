package com.github.xjtuwsn.cranemq.broker.registry;

import cn.hutool.core.util.StrUtil;
import com.github.xjtuwsn.cranemq.broker.BrokerController;
import com.github.xjtuwsn.cranemq.common.command.Header;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.command.payloads.req.MQUpdateTopicRequest;
import com.github.xjtuwsn.cranemq.common.command.types.RequestType;
import com.github.xjtuwsn.cranemq.common.command.types.RpcType;
import com.github.xjtuwsn.cranemq.common.exception.CraneClientException;
import com.github.xjtuwsn.cranemq.common.remote.RemoteClient;
import com.github.xjtuwsn.cranemq.common.remote.RemoteHook;
import com.github.xjtuwsn.cranemq.common.remote.WritableRegistry;
import com.github.xjtuwsn.cranemq.common.route.QueueData;
import com.github.xjtuwsn.cranemq.common.utils.TopicUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @project:cranemq
 * @file:SimpleWritableRegistry
 * @author:wsn
 * @create:2023/10/16-11:16
 * 默认的可写注册中心，将路由信息写道注册中心
 */
public class SimpleWritableRegistry implements WritableRegistry {
    private static final Logger log = LoggerFactory.getLogger(SimpleWritableRegistry.class);

    private RemoteClient remoteClient;
    private BrokerController brokerController;

    public SimpleWritableRegistry(BrokerController brokerController) {
        this.remoteClient = new RemoteClient();
        this.brokerController = brokerController;
    }
    @Override
    public void uploadRouteInfo(String brokerName, int brokerId, String address, Map<String, QueueData> queueDatas) {
        Header header = new Header(RequestType.UPDATE_TOPIC_REQUEST, RpcType.ONE_WAY, TopicUtil.generateUniqueID());
        MQUpdateTopicRequest mqUpdateTopicRequest = new MQUpdateTopicRequest(brokerName, brokerId, address, queueDatas);
        RemoteCommand remoteCommand = new RemoteCommand(header, mqUpdateTopicRequest);
        String registrys = this.brokerController.getBrokerConfig().getRegistry();
        if (StrUtil.isEmpty(registrys)) {
            log.error("No registry to connect");
            throw new CraneClientException("No registry to connect");
        }
        String[] registryList = registrys.split(";");
        for (String addr : registryList) {
            this.remoteClient.invoke(addr, remoteCommand);
        }
    }

    @Override
    public void append() {

    }
    @Override
    public void start() {
        this.remoteClient.start();
    }

    @Override
    public void shutdown() {
        this.remoteClient.shutdown();
    }

    @Override
    public void registerHook(RemoteHook hook) {

    }


}
