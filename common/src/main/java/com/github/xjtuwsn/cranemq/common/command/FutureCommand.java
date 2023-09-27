package com.github.xjtuwsn.cranemq.common.command;

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
public class FutureCommand implements Future<RemoteCommand> {
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return false;
    }

    @Override
    public RemoteCommand get() throws InterruptedException, ExecutionException {
        return null;
    }

    @Override
    public RemoteCommand get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return null;
    }
}
