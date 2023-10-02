package com.github.xjtuwsn.cranemq.common.net;

/**
 * @project:cranemq
 * @file:ProducerHook
 * @author:wsn
 * @create:2023/09/27-14:45
 */
public interface RemoteHook {

    void beforeMessage();

    void afterMessage();
}
