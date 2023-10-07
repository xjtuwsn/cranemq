package com.github.xjtuwsn.cranemq.common.remote;

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
