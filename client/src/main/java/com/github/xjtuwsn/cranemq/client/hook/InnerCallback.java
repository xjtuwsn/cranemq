package com.github.xjtuwsn.cranemq.client.hook;

import com.github.xjtuwsn.cranemq.client.producer.result.SendResult;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;

/**
 * @project:cranemq
 * @file:InnerCallback
 * @author:wsn
 * @create:2023/09/28-22:04
 */
public abstract class InnerCallback implements SendCallback {
    @Override
    public void onSuccess(SendResult result) {

    }

    @Override
    public void onFailure(Throwable reason) {

    }

    public abstract void onResponse(RemoteCommand remoteCommand);
}
