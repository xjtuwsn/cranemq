package com.github.xjtuwsn.cranemq.client.producer.impl;

import com.github.xjtuwsn.cranemq.client.hook.SendCallback;
import com.github.xjtuwsn.cranemq.common.command.FutureCommand;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @project:cranemq
 * @file:WrapperFutureCommand
 * @author:wsn
 * @create:2023/09/27-21:06
 */
public class WrapperFutureCommand {

    private FutureCommand futureCommand;
    private int maxRetryTime;
    private long timeout;
    private AtomicInteger currentRetryTime;
    private SendCallback callback;
    public WrapperFutureCommand(FutureCommand futureCommand, int maxRetryTime, long timeout, SendCallback callback) {
        this.futureCommand = futureCommand;
        this.maxRetryTime = maxRetryTime;
        this.timeout = timeout;
        this.callback = callback;
        currentRetryTime = new AtomicInteger(0);
    }

    public FutureCommand getFutureCommand() {
        return futureCommand;
    }

    public SendCallback getCallback() {
        return callback;
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
}
