package com.github.xjtuwsn.cranemq.broker.push;

import cn.hutool.core.collection.ConcurrentHashSet;
import cn.hutool.core.lang.Pair;
import com.github.xjtuwsn.cranemq.broker.BrokerController;
import com.github.xjtuwsn.cranemq.common.command.Header;
import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.command.payloads.req.MQPullMessageRequest;
import com.github.xjtuwsn.cranemq.common.command.payloads.resp.MQPullMessageResponse;
import com.github.xjtuwsn.cranemq.common.command.types.AcquireResultType;
import com.github.xjtuwsn.cranemq.common.command.types.ResponseType;
import com.github.xjtuwsn.cranemq.common.command.types.RpcType;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import com.github.xjtuwsn.cranemq.common.entity.ReadyMessage;
import com.github.xjtuwsn.cranemq.common.utils.BrokerUtil;
import com.github.xjtuwsn.cranemq.common.utils.TopicUtil;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Key;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @project:cranemq
 * @file:HoldRequestService
 * @author:wsn
 * @create:2023/10/10-09:51
 */
public class HoldRequestService {

    private static final Logger log = LoggerFactory.getLogger(HoldRequestService.class);
    // topic@group: [queueId: request]
    private volatile ConcurrentHashMap<String, ConcurrentHashMap<Integer, RequestWrapper>> requestTable = new ConcurrentHashMap<>();
    // topic: [topic@group]
    private ConcurrentHashMap<String, ConcurrentHashSet<String>> topicQueryTable = new ConcurrentHashMap<>();
    private BrokerController brokerController;
    private ExecutorService asyncReadService;
    private ScheduledExecutorService scanRequestTableService;

    public HoldRequestService(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.asyncReadService = new ThreadPoolExecutor(3, 6, 60L,
                TimeUnit.SECONDS, new LinkedBlockingDeque<>(2000),
                new ThreadFactory() {
                    AtomicInteger index = new AtomicInteger(0);
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "AsyncReadService NO." + index.getAndIncrement());
                    }
                });
        scanRequestTableService = new ScheduledThreadPoolExecutor(3);
    }
    public void tryHoldRequest(MQPullMessageRequest pullMessageRequest, Channel channel, String id) {
        RequestWrapper requestWrapper = new RequestWrapper(pullMessageRequest, channel, id);
        String topic = requestWrapper.getTopic(), group = requestWrapper.getGroup(), clientId = pullMessageRequest.getClientId();
        String key = BrokerUtil.holdRequestKey(topic, group, clientId);
        int queueId = requestWrapper.getQueueId();
        ConcurrentHashMap<Integer, RequestWrapper> temp = new ConcurrentHashMap<>();
        ConcurrentHashMap<Integer, RequestWrapper> map = requestTable.putIfAbsent(key, temp);
        if (map == null) {
            temp.put(queueId, requestWrapper);
        } else {
            map.put(queueId, requestWrapper);
        }

        ConcurrentHashSet<String> swap = new ConcurrentHashSet<>();
        ConcurrentHashSet<String> sets = topicQueryTable.putIfAbsent(topic, swap);
        if (sets == null) {
            swap.add(key);
        } else {
            sets.add(key);
        }

        long offset = 0L;
        if (pullMessageRequest.getOffset() == -1) {
            offset = brokerController.getOffsetManager().getOffsetInQueue(topic, group, queueId);
        } else {
            offset = pullMessageRequest.getOffset();
        }
        requestWrapper.setOffset(offset);

        long commitOffset = pullMessageRequest.getCommitOffset();
        brokerController.getOffsetManager().updateOffset(topic, group, queueId, commitOffset);
        this.asyncRead(requestWrapper);

    }

    public void awakeNow(List<Pair<String, Integer>> queues) {
        if (queues == null || queues.size() == 0) {
            return;
        }
        for (Pair<String, Integer> queue : queues) {
            String topic = queue.getKey();
            int queueId = queue.getValue();
            ConcurrentHashSet<String> groups = topicQueryTable.get(topic);
            if (groups != null && !groups.isEmpty()) {
                for (String key : groups) {
                    RequestWrapper wrapper = requestTable.get(key).get(queueId);
                    if (wrapper != null) {
                        asyncRead(wrapper);
                    }
                }
            }
        }
    }
    private void asyncRead(RequestWrapper wrapper) {
        this.asyncReadService.execute(() -> {
            readAndResopnse(wrapper);
        });
    }

    private void readAndResopnse(RequestWrapper wrapper) {
        long arriveTime = wrapper.getArriveTime();
        long now = System.currentTimeMillis();
        Header header = new Header(ResponseType.PULL_RESPONSE, RpcType.ONE_WAY, wrapper.getId());
        PayLoad payLoad = null;
        if (now - arriveTime >= brokerController.getBrokerConfig().getLongPollingTime()) {
            payLoad = new MQPullMessageResponse(AcquireResultType.NO_MESSAGE, wrapper.getGroup(), null, wrapper.getOffset());
        } else {
            Pair<Pair<List<ReadyMessage>, Long>, AcquireResultType> result = readFromFile(wrapper);
            if (result == null || result.getValue() != AcquireResultType.DONE) {
                return;
            }
            List<ReadyMessage> list = result.getKey().getKey();
            long nextOffset = result.getKey().getValue();
            payLoad = new MQPullMessageResponse(result.getValue(), wrapper.getGroup(), list, nextOffset);
        }
        RemoteCommand remoteCommand = new RemoteCommand(header, payLoad);

        if (wrapper.isOk() && wrapper.valid.get()) {
            synchronized (wrapper) {
                if (wrapper.valid.get()) {
                    wrapper.valid.set(false);
                    wrapper.getChannel().writeAndFlush(remoteCommand);
                }
            }

        }
        remove(wrapper);
    }

    private Pair<Pair<List<ReadyMessage>, Long>, AcquireResultType> readFromFile(RequestWrapper wrapper) {
        String topic = wrapper.getTopic(), group = wrapper.getGroup();
        int queueId = wrapper.getQueueId();
        long offset = wrapper.getOffset();
        Pair<Pair<List<ReadyMessage>, Long>, AcquireResultType> result = brokerController.getMessageStoreCenter()
                .read(topic, queueId, offset,
                brokerController.getPersistentConfig().getMaxSingleReadLength());
        return result;
    }

    private void scanTable() {
        for (Map.Entry<String, ConcurrentHashMap<Integer, RequestWrapper>> outter : requestTable.entrySet()) {
            ConcurrentHashMap<Integer, RequestWrapper> value = outter.getValue();
            for (Map.Entry<Integer, RequestWrapper> inner : value.entrySet()) {
                RequestWrapper wrapper = inner.getValue();
                // 通道已经关闭
                if (!wrapper.isOk()) {
                    value.remove(inner.getKey());
                    log.info("Channel has closed");
                    continue;
                }
                asyncRead(wrapper);
            }
        }
    }

    private void remove(RequestWrapper wrapper) {
        String key = wrapper.getKey();
        int queueId = wrapper.getQueueId();
        wrapper.valid.set(false);
        requestTable.get(key).remove(queueId);
    }
    public void start() {
        this.scanRequestTableService.scheduleAtFixedRate(() -> {
            scanTable();
        }, 100, 3 * 1000, TimeUnit.MILLISECONDS);
    }

    public void shutdown() {

    }

    class RequestWrapper {
        private String clientId;
        private String key;
        private String group;
        private String topic;
        private MessageQueue messageQueue;

        private long offset;
        private long arriveTime;
        private Channel channel;
        private String id;
        private AtomicBoolean valid = new AtomicBoolean(true);
        public RequestWrapper(MQPullMessageRequest request, Channel channel, String id) {
            this(request, channel, 0, id);
        }
        public RequestWrapper(MQPullMessageRequest request, Channel channel, long offset, String id) {
            this.clientId = request.getClientId();
            this.group = request.getGroupName();
            this.messageQueue = request.getMessageQueue();
            this.topic = request.getTopic();
            this.offset = offset;
            this.channel = channel;
            this.arriveTime = System.currentTimeMillis();
            this.key = BrokerUtil.holdRequestKey(topic, group, clientId);
            this.id = id;
        }

        public String getGroup() {
            return group;
        }

        public String getTopic() {
            return topic;
        }

        public int getQueueId() {
            return messageQueue.getQueueId();
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

        public long getOffset() {
            return offset;
        }

        public long getArriveTime() {
            return arriveTime;
        }

        public Channel getChannel() {
            return channel;
        }

        public void setOffset(long offset) {
            this.offset = offset;
        }

        public String getKey() {
            return key;
        }

        public String getId() {
            return id;
        }

        public boolean isOk() {
            return channel != null && channel.isActive();
        }

        @Override
        public String toString() {
            return "RequestWrapper{" +
                    "key='" + key + '\'' +
                    ", group='" + group + '\'' +
                    ", topic='" + topic + '\'' +
                    ", messageQueue=" + messageQueue +
                    ", offset=" + offset +
                    ", arriveTime=" + arriveTime +
                    ", id='" + id + '\'' +
                    '}';
        }
    }
}
