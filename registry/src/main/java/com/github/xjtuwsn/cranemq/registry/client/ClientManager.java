package com.github.xjtuwsn.cranemq.registry.client;

import com.github.xjtuwsn.cranemq.common.command.payloads.req.MQHeartBeatRequest;
import com.github.xjtuwsn.cranemq.common.remote.event.ChannelEventListener;
import com.github.xjtuwsn.cranemq.registry.RegistryController;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @project:cranemq
 * @file:ClientManager
 * @author:wsn
 * @create:2023/10/15-16:32
 */
public class ClientManager implements ChannelEventListener {
    private static final Logger log = LoggerFactory.getLogger(ClientManager.class);

    private RegistryController registryController;

    private ConcurrentHashMap<String, BrokerWrapper> brokerTable = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, String> brokerChannels = new ConcurrentHashMap<>();
    private ScheduledExecutorService scanTableService;

    public ClientManager(RegistryController registryController) {
        this.registryController = registryController;
        scanTableService = new ScheduledThreadPoolExecutor(1);
        scanTableService.scheduleAtFixedRate(() -> {
            this.scanTable();
        }, 500, 10 * 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public void onConnect(Channel channel) {
        log.info("Channel {} connected", channel);
    }

    @Override
    public void onIdle(Channel channel) {
        log.info("Channel {} idle", channel);
    }

    @Override
    public void onDisconnect(Channel channel) {
        log.info("Channel {} disConnect", channel);
        doRemove(channel);
    }
    @Override
    public void onException(Channel channel) {
        log.info("Channel {} exception", channel);
        doRemove(channel);
    }

    private void scanTable() {
        for (Map.Entry<String, BrokerWrapper> entry : brokerTable.entrySet()) {
            BrokerWrapper brokerWrapper = entry.getValue();
            if (brokerWrapper.isExpired()) {
                doRemove(brokerWrapper);
            }
        }
    }

    private void doRemove(Channel channel) {
        String key = getKey(channel);
        doRemove(brokerTable.get(key));
    }
    private void doRemove(BrokerWrapper brokerWrapper) {
        String brokerName = brokerWrapper.brokerName;
        int brokerId = brokerWrapper.brokerId;
        Channel channel = brokerWrapper.channel;
        this.registryController.getTopicInfoHolder().removerBrokerData(brokerName, brokerId);
        String key = getKey(channel);
        this.brokerChannels.remove(key);
        this.brokerTable.remove(brokerFlag(brokerName, brokerId));
        channel.close();

    }
    // TODO 在注册中心进行broker的管理，定期扫描删除过期，进行注册中心测试
    @Override
    public void onBrokerHeartBeat(Channel channel, String brokerName, int brokerId) {
        String key = getKey(channel);
        String broker = brokerFlag(brokerName, brokerId);
        BrokerWrapper brokerWrapper = brokerTable.get(broker);
        if (brokerWrapper == null) {
            brokerWrapper = new BrokerWrapper(brokerName, brokerId, channel);
            brokerTable.put(broker, brokerWrapper);
            brokerChannels.put(key, broker);
        }
        brokerWrapper.updateTime();
    }

    private String brokerFlag(String brokerName, int brokerId) {
        return brokerName + "-" + brokerId;
    }
    private String getKey(Channel channel) {
        return channel.id() + channel.remoteAddress().toString();
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

        public void updateTime() {
            this.lastCommTime = System.currentTimeMillis();
        }

        public boolean isExpired() {
            long gap = System.currentTimeMillis() - lastCommTime;
            return gap >= 100 * 1000;
        }
    }
}
