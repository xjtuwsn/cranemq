package com.github.xjtuwsn.cranemq.client.remote;

import com.github.xjtuwsn.cranemq.client.hook.PullCallback;
import com.github.xjtuwsn.cranemq.client.hook.SendCallback;
import com.github.xjtuwsn.cranemq.client.producer.MQSelector;
import com.github.xjtuwsn.cranemq.common.command.FutureCommand;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.entity.MessageQueue;
import lombok.ToString;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @project:cranemq
 * @file:WrapperFutureCommand
 * @author:wsn
 * @create:2023/09/27-21:06
 * 请求的最终包装类
 */
@ToString
public class WrapperFutureCommand {

    // future包装的请求
    private FutureCommand futureCommand;

    // 消息发往的主题
    private String topic;

    // 是否发往注册中心
    private boolean toRegistry;

    // 最大重试次数
    private int maxRetryTime;

    // 最大超时时间
    private long timeout;

    // 消息发送时间
    private AtomicLong startTime;

    // 重试次数
    private AtomicInteger currentRetryTime;

    // 异步发送回调
    private SendCallback callback;

    // 拉取消息回调
    private PullCallback pullCallback;

    // 队列选择器
    private MQSelector selector;

    // 选择器参数
    private Object arg;

    // 已经选择的队列
    private MessageQueue queuePicked;

    // 已经选择的远程地址
    private String address;

    public WrapperFutureCommand(FutureCommand futureCommand, String topic, long timeout, SendCallback callback) {
        this(futureCommand, -1, timeout, callback, topic);
    }
    public WrapperFutureCommand(FutureCommand futureCommand, String topic) {
        this(futureCommand, -1, -1, null, topic);
    }
    public WrapperFutureCommand(FutureCommand futureCommand, int maxRetryTime,
                                long timeout, SendCallback callback, String topic) {
        this.futureCommand = futureCommand;
        this.maxRetryTime = maxRetryTime;
        this.timeout = timeout;
        this.callback = callback;
        this.currentRetryTime = new AtomicInteger(0);
        this.startTime = new AtomicLong(0);
        this.topic = topic;
    }

    public FutureCommand getFutureCommand() {
        return futureCommand;
    }
    public void setResponse(RemoteCommand response) {
        this.futureCommand.setResponse(response);
    }
    public SendCallback getCallback() {
        return callback;
    }
    public void setStartTime(long startTime) {
        this.startTime.set(startTime);
    }
    public boolean isExpired() {
        return this.startTime.get() + this.timeout > System.currentTimeMillis();
    }
    public long getTimeout() {
        return timeout;
    }

    public void increaseRetryTime() {
        this.currentRetryTime.incrementAndGet();
    }
    public boolean isNeedRetry() {
        return this.currentRetryTime.get() < this.maxRetryTime;
    }
    public boolean cancel() {
        return futureCommand.cancel(true);
    }
    public boolean isDone() {
        return this.futureCommand.isDone();
    }

    public String getTopic() {
        return topic;
    }

    public boolean isToRegistry() {
        return toRegistry;
    }

    public void setToRegistry(boolean toRegistry) {
        this.toRegistry = toRegistry;
    }

    public MQSelector getSelector() {
        return selector;
    }

    public void setSelector(MQSelector selector) {
        this.selector = selector;
    }

    public Object getArg() {
        return arg;
    }

    public void setArg(Object arg) {
        this.arg = arg;
    }

    public PullCallback getPullCallback() {
        return pullCallback;
    }

    public void setPullCallback(PullCallback pullCallback) {
        this.pullCallback = pullCallback;
    }

    public MessageQueue getQueuePicked() {
        return queuePicked;
    }

    public void setQueuePicked(MessageQueue queuePicked) {
        this.queuePicked = queuePicked;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
