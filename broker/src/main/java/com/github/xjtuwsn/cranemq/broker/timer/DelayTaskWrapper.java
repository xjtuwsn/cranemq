package com.github.xjtuwsn.cranemq.broker.timer;

import java.util.concurrent.TimeUnit;

/**
 * @project:cranemq
 * @file:DelayTaskWrapper
 * @author:wsn
 * @create:2023/10/19-19:50
 */

/**
 * 延时任务的包装类
 * @param <T>
 */
public class DelayTaskWrapper<T extends Thread> {
    // 任务
    private T task;
    // 到期时间
    private long expirationTime;
    // 前驱和后继节点
    public DelayTaskWrapper<T> next, prev;

    public DelayTaskWrapper() {
    }

    public DelayTaskWrapper(T task, long expirationTime) {
        this.task = task;
        this.expirationTime = expirationTime;
    }

    public T getTask() {
        return task;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public boolean isExpired() {
        return expirationTime <= TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    }
}
