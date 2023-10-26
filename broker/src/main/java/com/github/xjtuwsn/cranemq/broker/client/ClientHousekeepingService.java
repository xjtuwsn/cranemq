package com.github.xjtuwsn.cranemq.broker.client;

import com.github.xjtuwsn.cranemq.broker.BrokerController;
import com.github.xjtuwsn.cranemq.common.command.Header;
import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.command.payloads.resp.MQNotifyChangedResponse;
import com.github.xjtuwsn.cranemq.common.command.types.ResponseType;
import com.github.xjtuwsn.cranemq.common.command.types.RpcType;
import com.github.xjtuwsn.cranemq.common.constant.MQConstant;
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
 * 进行客户端的管理
 */
public class ClientHousekeepingService implements ChannelEventListener, ConsumerGroupManager {
    private static final Logger log = LoggerFactory.getLogger(ClientHousekeepingService.class);
    private BrokerController brokerController;
    private ConcurrentHashMap<String, ClientWrapper> produceTable = new ConcurrentHashMap<>();
    // group: [clientId: client]
    private ConcurrentHashMap<String, ConcurrentHashMap<String, ClientWrapper>> consumerTable = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ConsumerInfo> groupProperty = new ConcurrentHashMap<>();

    private ConcurrentHashMap<String, CopyOnWriteArrayList<ClientWrapper>> channelTable = new ConcurrentHashMap<>();
    private ScheduledExecutorService scanInactiveService;
    private ExecutorService asyncNotifyConsumerService;
    private ExecutorService asyncSweepService;
    private AtomicInteger activeNumber;
    public ClientHousekeepingService(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.activeNumber = new AtomicInteger(0);
        this.scanInactiveService = new ScheduledThreadPoolExecutor(2);
        this.scanInactiveService.scheduleAtFixedRate(() -> {
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

    /**
     * 某个客户端断联时
     * @param channel
     */
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

    /**
     * 执行某个客户端的下线操作
     * @param channel
     */
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
                // 释放该客户端所有分布式锁
                brokerController.getClientLockMananger().releaseLock(group, clientId);
                // 通知消费者组变化
                consumerGroupChanged(group);

            }
        }
        channel.close();

        channelTable.remove(key);


    }

    /**
     * 生产者心跳
     * @param request
     * @param channel
     */
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

    /**
     * 消费者心跳，更新消费者组信息
     * @param request
     * @param channel
     */
    @Override
    public void onConsumerHeartBeat(MQHeartBeatRequest request, Channel channel) {
        Set<ConsumerInfo> consumerGroup = request.getConsumerGroup();
        String clientId = request.getClientId();
        boolean gray = request.isGrayConsumer();
        for (ConsumerInfo info : consumerGroup) {
            String cg = info.getConsumerGroup();
            boolean changed = false;
            ConcurrentHashMap<String, ClientWrapper> temp = new ConcurrentHashMap<>();
            ConcurrentHashMap<String, ClientWrapper> prev = consumerTable.putIfAbsent(cg, temp);
            if (prev == null) {
                changed = true;
            }
            groupProperty.put(cg, info);
            ConcurrentHashMap<String, ClientWrapper> map = consumerTable.get(cg);

            ClientWrapper clientWrapper = new ClientWrapper(channel, clientId, info, gray);
            ClientWrapper prevWrapper = map.putIfAbsent(clientId, clientWrapper);
            if (prevWrapper == null) {
                changed = true;
                saveChannel(channel, clientWrapper);
            }
            ClientWrapper cur = prevWrapper == null ? clientWrapper : prevWrapper;
            cur.updateCommTime(System.currentTimeMillis());

            // 如果消费者组发生变化，就通知同组所有消费者
            if (changed) {
                consumerGroupChanged(cg);
            }
        }
    }

    /**
     * 通知消费者组变更
     * @param groupName
     */
    private void consumerGroupChanged(String groupName) {
        log.info("Consumer cluster {} has changed", groupName);
        ConcurrentHashMap<String, ClientWrapper> map = consumerTable.get(groupName);

        Set<String> clients = new HashSet<>();
        for (Map.Entry<String, ClientWrapper> entry : map.entrySet()) {
            clients.add(entry.getKey());
        }

        // 通知
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

    /**
     * 扫描长期未通信客户端
     */
    private void scanInactiveClient() {
        Iterator<Map.Entry<String, ClientWrapper>> iterator = this.produceTable.entrySet().iterator();
        doRemove(iterator);
        Iterator<Map.Entry<String, ConcurrentHashMap<String, ClientWrapper>>> consumerIterator =
                this.consumerTable.entrySet().iterator();
        while (consumerIterator.hasNext()) {
            Iterator<Map.Entry<String, ClientWrapper>> subiterator = consumerIterator.next().getValue().
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
        Set<String> clients = new HashSet<>();
        for (Map.Entry<String, ClientWrapper> entry : map.entrySet()) {
            String clientId = entry.getKey();
            boolean gray = map.get(clientId).isGrayConsumer();
            clients.add(clientId + (gray ? MQConstant.GRAY_SUFFIX : ""));
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

        private boolean grayConsumer;

        public ClientWrapper(Channel channel, String clientId, Set<String> groups) {
            this.channel = channel;
            this.clientId = clientId;
            this.groups = groups;
            this.lastCommTime = System.currentTimeMillis();
            this.role = Role.PRODUCER;
        }
        public ClientWrapper(Channel channel, String clientId, ConsumerInfo info, boolean grayConsumer) {
            this.channel = channel;
            this.clientId = clientId;
            this.consumerInfo = info;
            this.lastCommTime = System.currentTimeMillis();
            this.role = Role.CONSUMER;
            this.grayConsumer = grayConsumer;
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

        public boolean isGrayConsumer() {
            return grayConsumer;
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
