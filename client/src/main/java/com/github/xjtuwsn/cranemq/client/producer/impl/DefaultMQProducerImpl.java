package com.github.xjtuwsn.cranemq.client.producer.impl;

import cn.hutool.core.util.StrUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.xjtuwsn.cranemq.client.hook.RemoteHook;
import com.github.xjtuwsn.cranemq.client.producer.DefaultMQProducer;
import com.github.xjtuwsn.cranemq.client.producer.MQProducerInner;
import com.github.xjtuwsn.cranemq.client.remote.ClienFactory;
import com.github.xjtuwsn.cranemq.client.remote.RemoteClient;
import com.github.xjtuwsn.cranemq.common.command.Header;
import com.github.xjtuwsn.cranemq.common.command.PayLoad;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.command.payloads.MQProduceRequest;
import com.github.xjtuwsn.cranemq.common.command.types.RequestType;
import com.github.xjtuwsn.cranemq.common.command.types.RpcType;
import com.github.xjtuwsn.cranemq.common.entity.Message;
import com.github.xjtuwsn.cranemq.common.exception.CraneClientException;
import com.github.xjtuwsn.cranemq.common.net.RemoteAddress;
import com.github.xjtuwsn.cranemq.common.utils.NetworkUtil;

import java.util.List;
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
        this.remoteClient = ClienFactory.newInstance().getOrCreate(this.clientID, this.address);
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

    }
    public void send(Message message) {
        Header header = new Header(RequestType.MESSAGE_PRODUCE_REQUEST, RpcType.ASYNC, "121212");
        PayLoad payLoad = new MQProduceRequest(message);
        RemoteCommand remoteCommand = new RemoteCommand(header, payLoad);
        this.remoteClient.invoke(remoteCommand);
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
    @Override
    public List<String> getBrokerAddress() {
        return null;
    }

    @Override
    public String fetechOrCreateTopic(int queueNumber) {
        return null;
    }
}
