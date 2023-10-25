package com.github.xjtuwsn.cranemq.broker.store;

import com.github.xjtuwsn.cranemq.broker.store.comm.AsyncRequest;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @project:cranemq
 * @file:CreateServiceThread
 * @author:wsn
 * @create:2023/10/05-11:03
 */

/**
 * 执行异步创建操作的抽象类
 * @author wsn
 */
public abstract class CreateServiceThread extends Thread {

    protected boolean isStop = false;
    // 存放创建请求
    protected LinkedBlockingQueue<AsyncRequest> requestQueue;
    // 保存已经发出的请求
    protected ConcurrentHashMap<String, AsyncRequest> requestTable = new ConcurrentHashMap<>();
    public CreateServiceThread() {
        this.requestQueue = new LinkedBlockingQueue<>(1000);
    }
    @Override
    public void run() {
        while (!isStop && createLoop()) {

        }
    }

    protected abstract boolean createLoop();
    protected abstract MappedFile putCreateRequest(int index);
    protected abstract MappedFile putCreateRequest(int index, String topic, int queueId);
    protected void setStop() {
        this.isStop = true;
    }
}
