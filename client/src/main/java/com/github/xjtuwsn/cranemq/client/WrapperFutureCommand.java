package com.github.xjtuwsn.cranemq.client;

import com.github.xjtuwsn.cranemq.client.hook.PullCallback;
import com.github.xjtuwsn.cranemq.client.hook.SendCallback;
import com.github.xjtuwsn.cranemq.client.producer.MQSelector;
import com.github.xjtuwsn.cranemq.common.command.FutureCommand;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import lombok.ToString;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @project:cranemq
 * @file:WrapperFutureCommand
 * @author:wsn
 * @create:2023/09/27-21:06
 */
@ToString
public class WrapperFutureCommand {

    private FutureCommand futureCommand;
    private String topic;
    private boolean toRegistery;
    private int maxRetryTime;
    private long timeout;
    private AtomicLong startTime;
    private AtomicInteger currentRetryTime;
    private SendCallback callback;
    private PullCallback pullCallback;
    private MQSelector selector;
    private Object arg;

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

    public boolean isToRegistery() {
        return toRegistery;
    }

    public void setToRegistery(boolean toRegistery) {
        this.toRegistery = toRegistery;
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
}
