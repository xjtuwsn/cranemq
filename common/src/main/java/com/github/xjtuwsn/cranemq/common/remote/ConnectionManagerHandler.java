package com.github.xjtuwsn.cranemq.common.remote;

import com.github.xjtuwsn.cranemq.common.remote.enums.ConnectionEventType;
import com.github.xjtuwsn.cranemq.common.remote.event.ConnectionEvent;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @project:cranemq
 * @file:ConnectionManager
 * @author:wsn
 * @create:2023/10/02-15:27
 */
@ChannelHandler.Sharable
public class ConnectionManagerHandler extends ChannelDuplexHandler {
    private static final Logger log = LoggerFactory.getLogger(ConnectionManagerHandler.class);
    private RemoteServer remoteServer;
    public ConnectionManagerHandler(RemoteServer remoteServer) {
        this.remoteServer = remoteServer;
    }
    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        log.info("Channel registered : {}", ctx);
        super.channelRegistered(ctx);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        log.info("Channel unregistered : {}", ctx);
        closeChannel(ctx);
        super.channelUnregistered(ctx);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.info("Channel active : {}", ctx);
        super.channelActive(ctx);
        if (this.remoteServer.hasListener()) {
            this.remoteServer.publishEvent(new ConnectionEvent(ConnectionEventType.CONNECT, ctx.channel()));
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.info("Channel inactive : {}", ctx);
        if (this.remoteServer.hasListener()) {
            this.remoteServer.publishEvent(new ConnectionEvent(ConnectionEventType.DISCONNECT, ctx.channel()));
        }
        closeChannel(ctx);
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (this.remoteServer.hasListener()) {
            this.remoteServer.publishEvent(new ConnectionEvent(ConnectionEventType.EXCEPTION, ctx.channel()));
        }
        closeChannel(ctx);
    }

    private void closeChannel(ChannelHandlerContext channelHandlerContext) {
        if (channelHandlerContext != null && channelHandlerContext.channel() != null) {
            channelHandlerContext.channel().close();
        }

    }
}
