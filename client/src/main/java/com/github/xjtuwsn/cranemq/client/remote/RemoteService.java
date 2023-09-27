package com.github.xjtuwsn.cranemq.client.remote;

import com.github.xjtuwsn.cranemq.client.hook.RemoteHook;

/**
 * @project:cranemq
 * @file:RemoteService
 * @author:wsn
 * @create:2023/09/27-14:58
 */
public interface RemoteService {

    void start();

    void shutdown();

    void registerHook(RemoteHook hook);
}
