package com.github.xjtuwsn.cranemq.common.command;

import com.github.xjtuwsn.cranemq.common.exception.CraneClientException;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @project:cranemq
 * @file:FutureCommand
 * @author:wsn
 * @create:2023/09/27-10:51
 */
@ToString
public class FutureCommand implements Future<RemoteCommand> {
    private static final Logger log = LoggerFactory.getLogger(FutureCommand.class);
    private RemoteCommand response;
    private RemoteCommand request;
    private String correlationId;
    private volatile boolean isDone;
    private volatile boolean isCancel;
    private final Object lock = new Object();

    public RemoteCommand getRequest() {
        return request;
    }

    public FutureCommand() {
    }

    public FutureCommand(RemoteCommand request) {
        this.request = request;
    }

    public void setRequest(RemoteCommand request) {
        this.request = request;
    }

    public void setResponse(RemoteCommand response) {
        this.response = response;
        synchronized (this.lock) {
            this.isDone = true;
            this.lock.notifyAll();
        }
    }
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (mayInterruptIfRunning) {
            synchronized (this.lock) {
                log.info("Request {} has timeout, and been canceled!", this.request);
                this.isCancel = true;
                this.lock.notifyAll();
            }
        }
        return true;
    }

    @Override
    public boolean isCancelled() {
        return isCancel;
    }

    @Override
    public boolean isDone() {
        return isDone;
    }

    @Override
    public RemoteCommand get() throws InterruptedException, ExecutionException {
        try {
            return get(-1, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new CraneClientException("Get oper timeout");
        }
    }

    @Override
    public RemoteCommand get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (!isDone) {
            synchronized (this.lock) {
                if (!isDone) {
                    if (timeout < 0) {
                        this.lock.wait();
                    } else {
                        long timeoutMillis =
                                (TimeUnit.MILLISECONDS == unit) ? timeout : TimeUnit.MILLISECONDS.convert(timeout, unit);
                        this.lock.wait(timeoutMillis);
                    }
                }
            }
        }
        if (!isDone) {
            throw new TimeoutException();
        }
        return this.response;
    }
}
