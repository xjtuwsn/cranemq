package com.github.xjtuwsn.cranemq.broker.client;

import com.github.xjtuwsn.cranemq.broker.BrokerController;
import com.github.xjtuwsn.cranemq.common.command.Header;
import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.command.payloads.resp.MQNotifyChangedResponse;
import com.github.xjtuwsn.cranemq.common.command.types.ResponseType;
import com.github.xjtuwsn.cranemq.common.command.types.RpcType;
import com.github.xjtuwsn.cranemq.common.consumer.ConsumerInfo;
import com.github.xjtuwsn.cranemq.common.remote.event.ChannelEventListener;
import com.github.xjtuwsn.cranemq.common.command.payloads.req.MQHeartBeatRequest;
import com.github.xjtuwsn.cranemq.common.utils.TopicUtil;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
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
    private ExecutorService asyncNotifyConsumerService;
    private AtomicInteger activeNumber;
    public ClientHousekeepingService(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.activeNumber = new AtomicInteger(0);
        this.scanUnactiveService = new ScheduledThreadPoolExecutor(2);
        this.scanUnactiveService.scheduleAtFixedRate(() -> {
            this.scanInactiveClient();
        }, 500, 20 * 1000, TimeUnit.MILLISECONDS);
        asyncNotifyConsumerService = new ThreadPoolExecutor(6, 12, 60L,
                TimeUnit.SECONDS, new LinkedBlockingDeque<>(1000),
                new ThreadFactory() {
                    AtomicInteger index = new AtomicInteger(0);
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "AsyncNotifyConsumerService NO." + index.getAndIncrement());
                    }
                });
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
        Set<String> groups = request.getProducerGroup();
        Set<ConsumerInfo> consumerGroup = request.getConsumerGroup();
        String clientId = request.getClientId();

        if (!this.produceTable.containsKey(clientId)) {
            this.produceTable.put(clientId, new ClientWrepper(channel, clientId, groups));
        }

        System.out.println(request);
        ClientWrepper clientWrepper = produceTable.get(clientId);
        clientWrepper.updateCommTime(System.currentTimeMillis());

    }

    @Override
    public void onConsumerHeartBeat(MQHeartBeatRequest request, Channel channel) {
        Set<ConsumerInfo> consumerGroup = request.getConsumerGroup();
        String clientId = request.getClientId();
        for (ConsumerInfo info : consumerGroup) {
            String cg = info.getConsumerGroup();
            boolean changed = false;
            if (!consumerTable.containsKey(cg)) {
                changed = true;
                consumerTable.put(cg, new ConcurrentHashMap<>());
            }
            ConcurrentHashMap<String, ClientWrepper> map = consumerTable.get(cg);
            if (!map.containsKey(clientId)) {
                changed = true;
                map.put(clientId, new ClientWrepper(channel, clientId, info));
            }
            if (changed) {
                consumerGroupChanged(cg);
            }
        }
    }
    private void consumerGroupChanged(String groupName) {
        log.info("Consumer cluster {} has changed", groupName);
        ConcurrentHashMap<String, ClientWrepper> map = consumerTable.get(groupName);
        Iterator<String> iterator = map.keys().asIterator();
        Set<String> clients = new HashSet<>();
        while (iterator.hasNext()) {
            clients.add(iterator.next());
        }
        for (Map.Entry<String, ClientWrepper> entry : map.entrySet()) {
            asyncNotifyConsumerService.execute(() -> {
                Header header = new Header(ResponseType.NOTIFY_CHAGED_RESPONSE, RpcType.ONE_WAY,
                        TopicUtil.generateUniqueID());
                PayLoad payLoad = new MQNotifyChangedResponse(groupName, clients);
                RemoteCommand remoteCommand = new RemoteCommand(header, payLoad);
                System.out.println(remoteCommand);
                entry.getValue().channel.writeAndFlush(remoteCommand);
            });
        }
    }
    private void scanInactiveClient() {
        Iterator<Map.Entry<String, ClientWrepper>> iterator = this.produceTable.entrySet().iterator();
        doRemove(iterator);
        Iterator<Map.Entry<String, ConcurrentHashMap<String, ClientWrepper>>> consumgerIntrator =
                this.consumerTable.entrySet().iterator();
        while (consumgerIntrator.hasNext()) {
            Iterator<Map.Entry<String, ClientWrepper>> subiterator = consumgerIntrator.next().getValue().
                    entrySet().iterator();
            doRemove(subiterator);
        }
    }
    private void doRemove(Iterator<Map.Entry<String, ClientWrepper>> iterator) {
        while (iterator.hasNext()) {
            Map.Entry<String, ClientWrepper> next = iterator.next();
            ClientWrepper clientWrepper = next.getValue();
            long now = System.currentTimeMillis();
            long last = clientWrepper.getLastCommTime();
            if (now - last >= brokerController.getBrokerConfig().getKeepAliveTime()) {
                log.info("Client {} has disconnected and will be removed", clientWrepper);
                clientWrepper.close();
                iterator.remove();
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
        private String clientId;
        private long lastCommTime;
        private Role role;
        private String group;
        private ConsumerInfo consumerInfo;
        private Set<String> groups;

        public ClientWrepper(Channel channel, String clientId, Set<String> groups) {
            this.channel = channel;
            this.clientId = clientId;
            this.groups = groups;
            this.lastCommTime = System.currentTimeMillis();
            this.role = Role.PRODUCER;
        }
        public ClientWrepper(Channel channel, String clientId, ConsumerInfo info) {
            this.channel = channel;
            this.clientId = clientId;
            this.consumerInfo = info;
            this.lastCommTime = System.currentTimeMillis();
            this.role = Role.CONSUMER;
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
                    ", clientId='" + clientId + '\'' +
                    ", lastCommTime=" + lastCommTime +
                    ", role=" + role +
                    '}';
        }
    }
}
