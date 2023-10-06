package com.github.xjtuwsn.cranemq.broker.client;

import cn.hutool.core.collection.ConcurrentHashSet;
import com.github.xjtuwsn.cranemq.broker.BrokerController;
import com.github.xjtuwsn.cranemq.broker.remote.ChannelEventListener;
import com.github.xjtuwsn.cranemq.common.command.payloads.MQHeartBeatRequest;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @project:cranemq
 * @file:ClientHousekeepingService
 * @author:wsn
 * @create:2023/10/02-19:51
 */
public class ClientHousekeepingService implements ChannelEventListener {
    private static final Logger log = LoggerFactory.getLogger(ClientHousekeepingService.class);
    private BrokerController brokerController;
    private ConcurrentHashMap<String, ClientWrepper> produceTable = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ConcurrentHashMap<String, ClientWrepper>> consumerTable = new ConcurrentHashMap<>();
    private ScheduledExecutorService scanUnactiveService;
    private AtomicInteger activeNumber;
    public ClientHousekeepingService(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.activeNumber = new AtomicInteger(0);
        this.scanUnactiveService = new ScheduledThreadPoolExecutor(2);
        this.scanUnactiveService.scheduleAtFixedRate(() -> {
            this.scanInactiveClient();
        }, 500, 20 * 1000, TimeUnit.MILLISECONDS);
    }
    @Override
    public void onConnect(Channel channel) {
        this.activeNumber.incrementAndGet();
    }

    @Override
    public void onDisconnect(Channel channel) {
        this.activeNumber.decrementAndGet();
    }

    @Override
    public void onIdle(Channel channel) {
        log.info("{} channel on Idle", channel);
    }

    @Override
    public void onException(Channel channel) {
        log.info("{} channel on Exception", channel);
    }

    @Override
    public void onProducerHeartBeat(MQHeartBeatRequest request, Channel channel) {
        String key = getUniqueKey(channel, request);
        Set<String> groups = request.getProducerGroup();
        String clientId = request.getClientId();
        if (!this.produceTable.containsKey(key)) {
            this.produceTable.put(key, new ClientWrepper(channel, key, clientId, groups));
        }
        ClientWrepper clientWrepper = produceTable.get(key);
        clientWrepper.updateCommTime(System.currentTimeMillis());

    }

    @Override
    public void onConsumerHeartBeat(MQHeartBeatRequest request, Channel channel) {

    }

    private void scanInactiveClient() {
        Iterator<Map.Entry<String, ClientWrepper>> iterator = this.produceTable.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, ClientWrepper> next = iterator.next();
            ClientWrepper clientWrepper = next.getValue();
            long now = System.currentTimeMillis();
            long last = clientWrepper.getLastCommTime();
            if (now - last >= brokerController.getBrokerConfig().getKeepAliveTime()) {
                log.info("Client {} has disconnected and will be removed", clientWrepper);
                clientWrepper.close();
                log.info("Before remove size {}", produceTable.size());
                iterator.remove();
                log.info("After remove size {}", produceTable.size());
            }
        }
    }
    private String getUniqueKey(Channel channel, MQHeartBeatRequest request) {
        String remoteAddress = channel.remoteAddress().toString();
        StringBuilder sb = new StringBuilder();
        sb.append(channel.remoteAddress()).append("&").append(channel.id()).append("&").append(request.getClientId());
        // remoteaddress&channelId&clientId
        return sb.toString();
    }
    static class ClientWrepper {
        enum Role {
            PRODUCER,
            CONSUMER
        }
        private Channel channel;
        private String key;
        private String clientId;
        private long lastCommTime;
        private Role role;
        private String group;
        private Set<String> groups;

        public ClientWrepper(Channel channel, String key, String clientId, Set<String> groups) {
            this.channel = channel;
            this.key = key;
            this.clientId = clientId;
            this.groups = groups;
            this.lastCommTime = System.currentTimeMillis();
            this.role = Role.PRODUCER;
        }

        public ClientWrepper(Channel channel, String key) {
            this(channel, key, null, null);

        }

        public void markProducer() {
            this.role = Role.PRODUCER;
        }

        public void markConsuemr() {
            this.role = Role.CONSUMER;
        }
        public void updateCommTime(long now) {
            this.lastCommTime = now;
        }
        public void close() {
            if (this.channel != null) {
                this.channel.close();
            }
        }

        public long getLastCommTime() {
            return lastCommTime;
        }

        @Override
        public String toString() {
            return "ClientWrepper{" +
                    "key='" + key + '\'' +
                    ", clientId='" + clientId + '\'' +
                    ", lastCommTime=" + lastCommTime +
                    ", role=" + role +
                    '}';
        }
    }
}
