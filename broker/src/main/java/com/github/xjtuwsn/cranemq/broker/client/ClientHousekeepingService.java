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

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @project:cranemq
 * @file:ClientHousekeepingService
 * @author:wsn
 * @create:2023/10/02-19:51
 */
public class ClientHousekeepingService implements ChannelEventListener, ConsumerGroupManager {
    private static final Logger log = LoggerFactory.getLogger(ClientHousekeepingService.class);
    private BrokerController brokerController;
    private ConcurrentHashMap<String, ClientWrapper> produceTable = new ConcurrentHashMap<>();
    // group: [clientId: client]
    private ConcurrentHashMap<String, ConcurrentHashMap<String, ClientWrapper>> consumerTable = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ConsumerInfo> groupProperity = new ConcurrentHashMap<>();

    private ConcurrentHashMap<String, CopyOnWriteArrayList<ClientWrapper>> channelTable = new ConcurrentHashMap<>();
    private ScheduledExecutorService scanUnactiveService;
    private ExecutorService asyncNotifyConsumerService;
    private ExecutorService asyncSweepService;
    private AtomicInteger activeNumber;
    public ClientHousekeepingService(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.activeNumber = new AtomicInteger(0);
        this.scanUnactiveService = new ScheduledThreadPoolExecutor(2);
        this.scanUnactiveService.scheduleAtFixedRate(() -> {
            this.scanInactiveClient();
        }, 500, 30 * 1000, TimeUnit.MILLISECONDS);
        asyncNotifyConsumerService = new ThreadPoolExecutor(6, 12, 60L,
                TimeUnit.SECONDS, new LinkedBlockingDeque<>(1000),
                new ThreadFactory() {
                    AtomicInteger index = new AtomicInteger(0);
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "AsyncNotifyConsumerService NO." + index.getAndIncrement());
                    }
                });
        asyncSweepService = new ThreadPoolExecutor(3, 6, 60L,
                TimeUnit.SECONDS, new LinkedBlockingDeque<>(1000),
                new ThreadFactory() {
                    AtomicInteger index = new AtomicInteger(0);
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "AsyncSweepService NO." + index.getAndIncrement());
                    }
                });
    }
    @Override
    public void onConnect(Channel channel) {
        this.activeNumber.incrementAndGet();
    }

    @Override
    public void onDisconnect(Channel channel) {
        asyncSweepService.execute(() -> {
            doCloseChannel(channel);
        });
        this.activeNumber.decrementAndGet();
    }

    @Override
    public void onIdle(Channel channel) {
        log.info("{} channel on Idle", channel);
    }

    @Override
    public void onException(Channel channel) {
        asyncSweepService.execute(() -> {
            doCloseChannel(channel);
        });
        log.info("{} channel on Exception", channel);
    }

    private void doCloseChannel(Channel channel) {
        if (channel == null) {
            return;
        }
        String key = channel.id() + channel.remoteAddress().toString();
        CopyOnWriteArrayList<ClientWrapper> list = channelTable.get(key);
        if (list == null || list.isEmpty()) {
            return;
        }

        for (ClientWrapper wrapper : list) {
            // 生产者，从表中移除
            String clientId = wrapper.getClientId();
            if (wrapper.isProducer()) {
                produceTable.remove(clientId);
            } else { // 消费者
                ConsumerInfo consumerInfo = wrapper.getConsumerInfo();
                String group = consumerInfo.getConsumerGroup();
                // 从生产者表中移除
                ConcurrentHashMap<String, ClientWrapper> map = consumerTable.get(group);
                if (map != null) {
                    map.remove(clientId);
                }
                // 通知消费者组变化
                consumerGroupChanged(group);

            }
        }
        channel.close();

        channelTable.remove(key);


    }

    @Override
    public void onProducerHeartBeat(MQHeartBeatRequest request, Channel channel) {
        Set<String> groups = request.getProducerGroup();
        Set<ConsumerInfo> consumerGroup = request.getConsumerGroup();
        String clientId = request.getClientId();

        if (!this.produceTable.containsKey(clientId)) {
            ClientWrapper clientWrapper = new ClientWrapper(channel, clientId, groups);
            this.produceTable.put(clientId, clientWrapper);
            saveChannel(channel, clientWrapper);
        }

        ClientWrapper clientWrapper = produceTable.get(clientId);
        clientWrapper.updateCommTime(System.currentTimeMillis());

    }

    private void saveChannel(Channel channel, ClientWrapper clientWrapper) {
        if (channel == null) {
            return;
        }
        String key = channel.id() + channel.remoteAddress().toString();
        CopyOnWriteArrayList<ClientWrapper> list = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<ClientWrapper> clientWrappers = channelTable.putIfAbsent(key, list);
        if (clientWrappers == null) {
            list.add(clientWrapper);
        } else {
            clientWrappers.add(clientWrapper);
        }
    }

    @Override
    public void onConsumerHeartBeat(MQHeartBeatRequest request, Channel channel) {
        Set<ConsumerInfo> consumerGroup = request.getConsumerGroup();
        String clientId = request.getClientId();
        for (ConsumerInfo info : consumerGroup) {
            String cg = info.getConsumerGroup();
            boolean changed = false;
            ConcurrentHashMap<String, ClientWrapper> temp = new ConcurrentHashMap<>();
            ConcurrentHashMap<String, ClientWrapper> prev = consumerTable.putIfAbsent(cg, temp);
            if (prev == null) {
                changed = true;
            }
            groupProperity.put(cg, info);
            ConcurrentHashMap<String, ClientWrapper> map = consumerTable.get(cg);

            ClientWrapper clientWrapper = new ClientWrapper(channel, clientId, info);
            ClientWrapper prevWrapper = map.putIfAbsent(clientId, clientWrapper);
            if (prevWrapper == null) {
                changed = true;
                saveChannel(channel, clientWrapper);
            }
            ClientWrapper cur = prevWrapper == null ? clientWrapper : prevWrapper;
            cur.updateCommTime(System.currentTimeMillis());

            if (changed) {
                consumerGroupChanged(cg);
            }
        }
    }
    private void consumerGroupChanged(String groupName) {
        log.info("Consumer cluster {} has changed", groupName);
        ConcurrentHashMap<String, ClientWrapper> map = consumerTable.get(groupName);
        Iterator<String> iterator = map.keys().asIterator();
        Set<String> clients = new HashSet<>();
        while (iterator.hasNext()) {
            clients.add(iterator.next());
        }
        for (Map.Entry<String, ClientWrapper> entry : map.entrySet()) {
            asyncNotifyConsumerService.execute(() -> {
                Header header = new Header(ResponseType.NOTIFY_CHAGED_RESPONSE, RpcType.ONE_WAY,
                        TopicUtil.generateUniqueID());
                PayLoad payLoad = new MQNotifyChangedResponse(groupName, clients);
                RemoteCommand remoteCommand = new RemoteCommand(header, payLoad);
                entry.getValue().channel.writeAndFlush(remoteCommand);
            });
        }
    }
    private void scanInactiveClient() {
        Iterator<Map.Entry<String, ClientWrapper>> iterator = this.produceTable.entrySet().iterator();
        doRemove(iterator);
        Iterator<Map.Entry<String, ConcurrentHashMap<String, ClientWrapper>>> consumgerIntrator =
                this.consumerTable.entrySet().iterator();
        while (consumgerIntrator.hasNext()) {
            Iterator<Map.Entry<String, ClientWrapper>> subiterator = consumgerIntrator.next().getValue().
                    entrySet().iterator();
            doRemove(subiterator);
        }
    }
    private void doRemove(Iterator<Map.Entry<String, ClientWrapper>> iterator) {
        while (iterator.hasNext()) {
            Map.Entry<String, ClientWrapper> next = iterator.next();
            ClientWrapper clientWrapper = next.getValue();
            long now = System.currentTimeMillis();
            long last = clientWrapper.getLastCommTime();
            if (now - last >= brokerController.getBrokerConfig().getKeepAliveTime()) {
                log.info("Client {} has disconnected and will be removed", clientWrapper);
                clientWrapper.close();
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

    @Override
    public Set<String> getGroupClients(String group) {
        ConcurrentHashMap<String, ClientWrapper> map = consumerTable.get(group);
        Iterator<String> iterator = map.keys().asIterator();
        Set<String> clients = new HashSet<>();
        while (iterator.hasNext()) {
            clients.add(iterator.next());
        }
        return clients;
    }

    static class ClientWrapper {
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

        public ClientWrapper(Channel channel, String clientId, Set<String> groups) {
            this.channel = channel;
            this.clientId = clientId;
            this.groups = groups;
            this.lastCommTime = System.currentTimeMillis();
            this.role = Role.PRODUCER;
        }
        public ClientWrapper(Channel channel, String clientId, ConsumerInfo info) {
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

        public boolean isProducer() {
            return this.role == Role.PRODUCER;
        }

        public boolean isConsumer() {
            return this.role == Role.CONSUMER;
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

        public String getClientId() {
            return clientId;
        }

        public ConsumerInfo getConsumerInfo() {
            return consumerInfo;
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
