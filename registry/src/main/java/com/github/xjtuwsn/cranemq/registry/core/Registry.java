package com.github.xjtuwsn.cranemq.registry.core;

import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.remote.codec.NettyDecoder;
import com.github.xjtuwsn.cranemq.common.remote.codec.NettyEncoder;
import com.github.xjtuwsn.cranemq.common.remote.serialize.impl.Hessian1Serializer;
import com.github.xjtuwsn.cranemq.registry.handler.RegistryHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @project:cranemq
 * @file:Register
 * @author:wsn
 * @create:2023/09/29-14:37
 */
public class Registry {
    Logger log = LoggerFactory.getLogger(Registry.class);
    private final int port = 11111;
    private EventLoopGroup bossGroup;
    // 工作事件循环
    private EventLoopGroup workerGroup;
    public void start() {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(3);
        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline()
                                    .addLast(new NettyDecoder(RemoteCommand.class, new Hessian1Serializer()))
                                    .addLast(new NettyEncoder(RemoteCommand.class, new Hessian1Serializer()))
                                    .addLast(new RegistryHandler());
                        }
                    });
            ChannelFuture channelFuture = serverBootstrap.bind(port).sync();
            log.info("Broker bind in port {} finished.", port);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
