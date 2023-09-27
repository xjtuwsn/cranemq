package com.github.xjtuwsn.cranemq.client.producer.impl;

import cn.hutool.core.util.StrUtil;
import com.github.xjtuwsn.cranemq.client.hook.SendCallback;
import com.github.xjtuwsn.cranemq.client.producer.SendResult;
import com.github.xjtuwsn.cranemq.common.command.*;
import com.github.xjtuwsn.cranemq.common.command.payloads.MQBachProduceRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.xjtuwsn.cranemq.client.hook.RemoteHook;
import com.github.xjtuwsn.cranemq.client.producer.DefaultMQProducer;
import com.github.xjtuwsn.cranemq.client.producer.MQProducerInner;
import com.github.xjtuwsn.cranemq.client.remote.ClienFactory;
import com.github.xjtuwsn.cranemq.client.remote.RemoteClient;
import com.github.xjtuwsn.cranemq.common.command.payloads.MQProduceRequest;
import com.github.xjtuwsn.cranemq.common.command.types.RequestType;
import com.github.xjtuwsn.cranemq.common.command.types.RpcType;
import com.github.xjtuwsn.cranemq.common.entity.Message;
import com.github.xjtuwsn.cranemq.common.exception.CraneClientException;
import com.github.xjtuwsn.cranemq.common.net.RemoteAddress;
import com.github.xjtuwsn.cranemq.common.utils.NetworkUtil;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @project:cranemq
 * @file:DefaultMQProducerImpl
 * @author:wsn
 * @create:2023/09/27-14:38
 */
public class DefaultMQProducerImpl implements MQProducerInner {

    private static final Logger log = LoggerFactory.getLogger(DefaultMQProducer.class);

    private DefaultMQProducer defaultMQProducer;

    private RemoteHook hook;

    private ExecutorService asyncSendThreadPool;

    private ScheduledExecutorService timerService;

    private RemoteClient remoteClient;
    private RemoteAddress address;
    private String clientID;
    private int coreSize = 3;

    private int maxSize = 5;
    /**
     * 0: created
     * 1: started
     * 2: failed
     */
    private volatile AtomicInteger state;

    private static volatile ConcurrentHashMap<String, WrapperFutureCommand> requestTable = new ConcurrentHashMap<>();

    public DefaultMQProducerImpl(DefaultMQProducer defaultMQProducer, RemoteHook hook) {
        this.defaultMQProducer = defaultMQProducer;
        this.hook = hook;

        state = new AtomicInteger(0);
    }

    public void start() throws CraneClientException {
        if (this.state.get() != 0) {
            log.error("DefaultMQProducerImpl has already been started or faied!");
            throw new CraneClientException("DefaultMQProducerImpl has already been started or faied!");
        }
        this.checkConfig();
        this.address = this.defaultMQProducer.getBrokerAddress();
        this.clientID = buildClientID();
        this.remoteClient = ClienFactory.newInstance().getOrCreate(this.clientID, this);
        this.remoteClient.registerHook(hook);
        this.remoteClient.start();
        this.asyncSendThreadPool = new ThreadPoolExecutor(coreSize,
                maxSize,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(100),
                new ThreadFactory() {
                    AtomicInteger count = new AtomicInteger(0);
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "AsyncSendThreadPool no." + count.getAndIncrement());
                    }
                },
                new ThreadPoolExecutor.AbortPolicy());
        this.timerService = Executors.newScheduledThreadPool(2, new ThreadFactory() {
            AtomicInteger count = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "ScheduledThreadPool no." + count.getAndIncrement());
            }
        });

    }

    public void send(Message message) {
        Header header = new Header(RequestType.MESSAGE_PRODUCE_REQUEST, RpcType.ASYNC, "121212");
        PayLoad payLoad = new MQProduceRequest(message);
        RemoteCommand remoteCommand = new RemoteCommand(header, payLoad);
//        this.remoteClient.invoke(remoteCommand);
    }

    public SendResult sendSync(long timeout, boolean isOneWay, Message... messages) throws CraneClientException {
        if (messages == null || messages.length == 0) {
            throw new CraneClientException("Message cannot be empty!");
        }
        WrapperFutureCommand wrappered = buildRequest(RpcType.SYNC, null, timeout,  messages);
        if (isOneWay) {
            wrappered.getFutureCommand().getRequest().getHeader().setRpcType(RpcType.ONE_WAY);
        }
        if (wrappered == null) {
            throw new CraneClientException("Create Request error!");
        }
        FutureCommand futureCommand = wrappered.getFutureCommand();
        this.asyncSendThreadPool.execute(() -> {
            sendCore(wrappered, null);
        });
        if (isOneWay) return null;
        try {
            RemoteCommand response = futureCommand.get();
            log.info("Sync method get response: {}", response);
        } catch (InterruptedException | ExecutionException e) {
            throw new CraneClientException("Request timeout, has retry for max time");
        }
        return null;
    }

    public void sendAsync(SendCallback callback, long timeout, Message... messages) {
        if (messages == null || messages.length == 0) {
            throw new CraneClientException("Message cannot be empty!");
        }
        WrapperFutureCommand remoteCommand = buildRequest(RpcType.ASYNC, callback, timeout, messages);
        if (remoteCommand == null) {
            throw new CraneClientException("Create Request error!");
        }
        this.asyncSendThreadPool.execute(() -> {
            sendCore(remoteCommand, callback);
        });
    }
    private void sendCore(final WrapperFutureCommand wrappered, SendCallback callback) {
        RemoteCommand remoteCommand = wrappered.getFutureCommand().getRequest();
        RpcType rpcType = remoteCommand.getHeader().getRpcType();
        String correlationID = remoteCommand.getHeader().getCorrelationId();
        if (rpcType != RpcType.ONE_WAY) {
            requestTable.putIfAbsent(correlationID, wrappered);
            if (wrappered.getTimeout() > 0) {
                this.timerService.schedule(() -> {
                    if (wrappered.isDone()) {
                        log.info("{} has aready done, wont do timeout", correlationID);
                        requestTable.remove(correlationID);
                        return;
                    }
                    log.warn("Request {} has timeout", correlationID);
                    if (!wrappered.isNeedRetry()) {
                        log.warn("Request {} has timeout for max retry time", correlationID);
                        wrappered.cancel();
                        wrappered.getCallback().onFailure(new TimeoutException("Timeout"));
                        throw new CraneClientException("Request timeout, has retry for max time");
                    }
                    wrappered.increaseRetryTime();
                    this.asyncSendThreadPool.execute(() -> {
                        log.info("Request {} do retry", correlationID);
                        sendCore(wrappered, callback);
                    });

                }, wrappered.getTimeout(), TimeUnit.MILLISECONDS);
            }
        }
        this.remoteClient.invoke(remoteCommand);

    }

    private WrapperFutureCommand buildRequest(RpcType rpcType, SendCallback callback, long timeout, Message... messages) {
        String correlationID = generateUniqueID();
        Header header = new Header(RequestType.MESSAGE_PRODUCE_REQUEST, RpcType.SYNC, correlationID);
        PayLoad payLoad = null;
        if (messages.length == 1) {
            payLoad = new MQProduceRequest(messages[0]);
        } else {
            payLoad = new MQBachProduceRequest(Arrays.asList(messages));
        }
        RemoteCommand remoteCommand = new RemoteCommand(header, payLoad);
        FutureCommand futureCommand = new FutureCommand();
        futureCommand.setRequest(remoteCommand);
        WrapperFutureCommand wrappered = new WrapperFutureCommand(futureCommand,
                this.defaultMQProducer.getMaxRetryTime(), timeout, callback);
        return wrappered;
    }

    private String generateUniqueID() {
        String id = UUID.randomUUID().toString().replaceAll("-", "").substring(0, 16);
        return id;
    }
    public void close() throws CraneClientException {
        this.asyncSendThreadPool.shutdown();
        this.remoteClient.shutdown();
    }

    private String buildClientID() {
        String ip = NetworkUtil.getLocalAddress();
        StringBuilder id = new StringBuilder();
        id.append(ip).append("@").append(this.defaultMQProducer.getTopic())
                .append("@").append(this.defaultMQProducer.getGroup());
        return id.toString();
    }

    private void checkConfig() throws CraneClientException {
        if (StrUtil.isEmpty(this.defaultMQProducer.getTopic())) {
            throw new CraneClientException("Topic name cannot be null");
        }
        if (StrUtil.isEmpty(this.defaultMQProducer.getGroup())) {
            throw new CraneClientException("Group name cannot be null");
        }
        if (StrUtil.isEmpty(this.defaultMQProducer.getBrokerAddress().getAddress())) {
            throw new CraneClientException("Brokder address cannot be null");
        }
        if (this.defaultMQProducer.getMaxRetryTime() < 0) {
            throw new CraneClientException("Max Retry Time cannot be negtive");
        }
    }

    public RemoteAddress getAddress() {
        return address;
    }

    public void setAddress(RemoteAddress address) {
        this.address = address;
    }

    @Override
    public List<String> getBrokerAddress() {
        return null;
    }

    @Override
    public String fetechOrCreateTopic(int queueNumber) {
        return null;
    }
}
