package xjtu.wsn.cranemq.broker.core;

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
import xjtu.wsn.cranemq.broker.handler.BaseHandler;
import xjtu.wsn.cranemq.common.net.codec.NettyDecoder;
import xjtu.wsn.cranemq.common.net.serialize.impl.Hessian1Serializer;
import xjtu.wsn.cranemq.common.request.BaseRequest;

/**
 * @project:cranemq
 * @file:MqBroker
 * @author:wsn
 * @create:2023/09/26-21:28
 */
public class MqBroker {
    Logger log = LoggerFactory.getLogger(MqBroker.class);
    private final int port = 9999;
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
                                    .addLast(new NettyDecoder(BaseRequest.class, new Hessian1Serializer()))
                                    .addLast(new BaseHandler());
                        }
                    });
            ChannelFuture channelFuture = serverBootstrap.bind(port).sync();
            log.info("Broker bind in port {} finished.", port);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
