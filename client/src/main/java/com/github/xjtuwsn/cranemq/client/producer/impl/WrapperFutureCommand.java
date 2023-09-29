package com.github.xjtuwsn.cranemq.client.producer.impl;

import com.github.xjtuwsn.cranemq.client.hook.SendCallback;
import com.github.xjtuwsn.cranemq.common.command.FutureCommand;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @project:cranemq
 * @file:WrapperFutureCommand
 * @author:wsn
 * @create:2023/09/27-21:06
 */
public class WrapperFutureCommand {

    private FutureCommand futureCommand;
    private String topic;
    private int maxRetryTime;
    private long timeout;
    private AtomicLong startTime;
    private AtomicInteger currentRetryTime;
    private SendCallback callback;

    public WrapperFutureCommand(FutureCommand futureCommand, String topic, long timeout, SendCallback callback) {
        this(futureCommand, -1, timeout, callback, topic);
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
}
