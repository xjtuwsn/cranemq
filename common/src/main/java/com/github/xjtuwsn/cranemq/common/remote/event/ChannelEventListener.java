package com.github.xjtuwsn.cranemq.common.remote.event;

import com.github.xjtuwsn.cranemq.common.command.payloads.req.MQHeartBeatRequest;
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
    void onProducerHeartBeat(MQHeartBeatRequest request, Channel channel);
    void onConsumerHeartBeat(MQHeartBeatRequest request, Channel channel);
    default void onBrokerHeartBeat(Channel channel, String brokerName, int brokerId) {}
}
