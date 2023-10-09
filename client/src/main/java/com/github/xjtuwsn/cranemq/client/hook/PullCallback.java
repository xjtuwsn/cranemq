package com.github.xjtuwsn.cranemq.client.hook;

import com.github.xjtuwsn.cranemq.client.consumer.PullResult;

/**
 * @project:cranemq
 * @file:PullCallback
 * @author:wsn
 * @create:2023/10/09-10:33
 */
public interface PullCallback {

    void onSuccess(PullResult pullResult);

    void onException(Throwable cause);
}
