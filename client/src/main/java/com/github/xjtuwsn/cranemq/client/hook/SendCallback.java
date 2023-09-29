package com.github.xjtuwsn.cranemq.client.hook;

import com.github.xjtuwsn.cranemq.client.producer.result.SendResult;

/**
 * @project:cranemq
 * @file:SendCallback
 * @author:wsn
 * @create:2023/09/27-19:45
 */
public interface SendCallback {

    void onSuccess(SendResult result);

    void onFailure(Throwable reason);
}
