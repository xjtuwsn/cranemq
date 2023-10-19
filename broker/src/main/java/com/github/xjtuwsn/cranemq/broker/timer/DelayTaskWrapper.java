package com.github.xjtuwsn.cranemq.broker.timer;

import java.util.concurrent.TimeUnit;

/**
 * @project:cranemq
 * @file:DelayTaskWrapper
 * @author:wsn
 * @create:2023/10/19-19:50
 */
public class DelayTaskWrapper<T extends Thread> {
    private T task;
    private long expiratonTime;
    public DelayTaskWrapper<T> next, prev;

    public DelayTaskWrapper() {
    }

    public DelayTaskWrapper(T task, long expiratonTime) {
        this.task = task;
        this.expiratonTime = expiratonTime;
    }

    public T getTask() {
        return task;
    }

    public long getExpiratonTime() {
        return expiratonTime;
    }

    public boolean isExpired() {
        return expiratonTime <= TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    }
}
