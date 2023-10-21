package com.github.xjtuwsn.cranemq.common.remote.processor;

import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.remote.RemoteHook;
import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.ExecutorService;

/**
 * @project:cranemq
 * @file:BaseProcessor
 * @author:wsn
 * @create:2023/10/07-14:39
 */
public interface BaseProcessor {
    // ---------------------- Producer ----------------------
    default void processMessageProduceResopnse(RemoteCommand remoteCommand,
                                       ExecutorService asyncHookService,
                                       RemoteHook hook) {}

    default void processCreateTopicResponse(RemoteCommand remoteCommand, ExecutorService asyncHookService) {}

    // ---------------------- Consumer ----------------------
    default void processSimplePullResponse(RemoteCommand remoteCommand, ExecutorService asyncHookService,
                                           RemoteHook hook) {}
    default void processNotifyChangedResponse(RemoteCommand remoteCommand, ExecutorService asyncHookService) {}

    default void processPullResponse(RemoteCommand remoteCommand, ExecutorService asyncHookService) {}

    default void processQueryResponse(RemoteCommand remoteCommand, ExecutorService asyncHookService) {}

    default void processLockResponse(RemoteCommand remoteCommand, ExecutorService asyncHookService) {}

    // ---------------------- Consumer and Producer ----------------------
    default void processUpdateTopicResponse(RemoteCommand remoteCommand, ExecutorService asyncHookService) {}


    // ---------------------- Broker ----------------------
    default void processProduceMessage(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {}
    default void processCreateTopic(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {}
    default void processHeartBeat(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {}
    default void processSimplePull(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {}

    default void processPullRequest(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {}

    default void processQueryRequest(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {}

    default void processRecordOffsetRequest(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {}
    default void processLockRequest(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {}

    default void processSendBackRequest(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {}

    // ---------------------- Registry ----------------------

    default void processQueryRouteRequest(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {}

    default void processUpdateRequest(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {}
}
