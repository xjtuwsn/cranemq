package com.github.xjtuwsn.cranemq.broker.client;

import io.netty.channel.Channel;

/**
 * @project:cranemq
 * @file:ClientChannelInfo
 * @author:wsn
 * @create:2023/10/02-16:52
 */
public class ClientChannelInfo {
    private Channel channel;
    private int clientId;
    private volatile long lastUpdateTime = System.currentTimeMillis();

}
