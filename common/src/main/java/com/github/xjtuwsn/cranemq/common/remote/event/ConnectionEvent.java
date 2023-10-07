package com.github.xjtuwsn.cranemq.common.remote.event;

import com.github.xjtuwsn.cranemq.common.remote.enums.ConnectionEventType;
import com.github.xjtuwsn.cranemq.common.command.payloads.req.MQHeartBeatRequest;
import io.netty.channel.Channel;

/**
 * @project:cranemq
 * @file:ConnectionEvent
 * @author:wsn
 * @create:2023/10/02-17:22
 */
public class ConnectionEvent {
    private ConnectionEventType eventType;
    private Channel channel;
    private MQHeartBeatRequest heartBeatRequest;

    public ConnectionEvent(ConnectionEventType eventType, Channel channel) {
        this(eventType, channel, null);
    }

    public ConnectionEvent(ConnectionEventType eventType, Channel channel, MQHeartBeatRequest heartBeatRequest) {
        this.eventType = eventType;
        this.channel = channel;
        this.heartBeatRequest = heartBeatRequest;
    }

    public ConnectionEventType getEventType() {
        return eventType;
    }

    public MQHeartBeatRequest getHeartBeatRequest() {
        return heartBeatRequest;
    }

    public Channel getChannel() {
        return channel;
    }
}
