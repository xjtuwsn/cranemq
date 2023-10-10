package com.github.xjtuwsn.cranemq.client.consumer.push;

import com.github.xjtuwsn.cranemq.client.remote.ClientInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @project:cranemq
 * @file:PullMessageService
 * @author:wsn
 * @create:2023/10/08-15:14
 */
public class PullMessageService extends Thread {

    private static final Logger log = LoggerFactory.getLogger(PullMessageService.class);

    private LinkedBlockingQueue<PullRequest> requestQueue = new LinkedBlockingQueue<>(5000);
    private ClientInstance clientInstance;
    private ScheduledExecutorService delayPutPool;
    private boolean isStop = false;
    public PullMessageService(ClientInstance clientInstance) {
        this.clientInstance = clientInstance;
        this.delayPutPool = new ScheduledThreadPoolExecutor(4);
    }
    @Override
    public void run() {
        while (!isStop) {
            parseRequest();
        }
    }

    private void parseRequest() {
        try {

            PullRequest request = requestQueue.take();
            // log.warn("Take the request {}", request);
            clientInstance.getPushConsumerByGroup(request.getGroupName()).pull(request);
        } catch (InterruptedException e) {
            log.warn("Take request has been Interrupted");
        }
    }
    public void putRequestNow(PullRequest pullRequest) {

        try {
            this.requestQueue.put(pullRequest);
        } catch (InterruptedException e) {
            log.error("Put request has been Interrupted");
        }
    }
    public void putRequestDelay(PullRequest pullRequest, long millis) {
        this.delayPutPool.schedule(() -> {
            putRequestNow(pullRequest);
        }, millis, TimeUnit.MILLISECONDS);
    }
    public void setStop() {
        this.isStop = true;
    }
}
