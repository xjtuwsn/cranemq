package com.github.xjtuwsn.cranemq.broker.remote;

import io.netty.channel.Channel;

/**
 * @project:cranemq
 * @file:ChannelEventListener
 * @author:wsn
 * @create:2023/10/02-19:47
 */
public interface ChannelEventListener {

    void onConnect(Channel channel);
    void onDisconnect(Channel channel);
    void onIdle(Channel channel);
    void onException(Channel channel);
}
