package com.github.xjtuwsn.cranemq.registry.client;

import com.github.xjtuwsn.cranemq.common.command.payloads.req.MQHeartBeatRequest;
import com.github.xjtuwsn.cranemq.common.remote.event.ChannelEventListener;
import com.github.xjtuwsn.cranemq.registry.RegistryController;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @project:cranemq
 * @file:ClientManager
 * @author:wsn
 * @create:2023/10/15-16:32
 */
public class ClientManager implements ChannelEventListener {
    private static final Logger log = LoggerFactory.getLogger(ClientManager.class);

    private RegistryController registryController;

    public ClientManager(RegistryController registryController) {
        this.registryController = registryController;
    }

    @Override
    public void onConnect(Channel channel) {
        log.info("Channel {} connected", channel);
    }

    @Override
    public void onDisconnect(Channel channel) {

    }

    @Override
    public void onIdle(Channel channel) {
        log.info("Channel {} idle", channel);
    }

    @Override
    public void onException(Channel channel) {

    }
    // TODO 在注册中心进行broker的管理，定期扫描删除过期，进行注册中心测试
    @Override
    public void onBrokerHeartBeat(Channel channel, String brokerName, int brokerId) {

    }

    @Override
    public void onProducerHeartBeat(MQHeartBeatRequest request, Channel channel) {

    }

    @Override
    public void onConsumerHeartBeat(MQHeartBeatRequest request, Channel channel) {

    }

    class BrokerWrapper {
        private String brokerName;
        private int brokerId;
        private Channel channel;

        private long lastCommTime;

        public BrokerWrapper(String brokerName, int brokerId, Channel channel) {
            this.brokerName = brokerName;
            this.brokerId = brokerId;
            this.channel = channel;
            this.lastCommTime = System.currentTimeMillis();
        }
    }
}
