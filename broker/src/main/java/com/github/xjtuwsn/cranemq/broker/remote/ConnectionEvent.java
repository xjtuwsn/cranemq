package com.github.xjtuwsn.cranemq.broker.remote;

import com.github.xjtuwsn.cranemq.broker.enums.ConnectionEventType;
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

    public ConnectionEvent(ConnectionEventType eventType, Channel channel) {
        this.eventType = eventType;
        this.channel = channel;
    }

    public ConnectionEventType getEventType() {
        return eventType;
    }

    public Channel getChannel() {
        return channel;
    }
}
