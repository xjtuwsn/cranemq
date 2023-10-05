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
public abstract class CreateServiceThread extends Thread {

    protected boolean isStop = false;
    protected LinkedBlockingQueue<AsyncRequest> requestQueue;
    protected ConcurrentHashMap<String, AsyncRequest> requestTable = new ConcurrentHashMap<>();
    public CreateServiceThread() {
        this.requestQueue = new LinkedBlockingQueue<>(500);
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
