package com.github.xjtuwsn.cranemq.client.remote;

import com.github.xjtuwsn.cranemq.client.producer.impl.DefaultMQProducerImpl;
import com.github.xjtuwsn.cranemq.common.command.types.RpcType;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.xjtuwsn.cranemq.client.hook.RemoteHook;
import com.github.xjtuwsn.cranemq.client.remote.handler.BaseProducerHandler;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.net.RemoteAddress;
import com.github.xjtuwsn.cranemq.common.net.codec.NettyDecoder;
import com.github.xjtuwsn.cranemq.common.net.codec.NettyEncoder;
import com.github.xjtuwsn.cranemq.common.net.serialize.impl.Hessian1Serializer;

/**
 * @project:cranemq
 * @file:RemoteClient
 * @author:wsn
 * @create:2023/09/27-14:59
 */
public class RemoteClient implements RemoteService {

    private static final Logger log= LoggerFactory.getLogger(RemoteClient.class);
    private RemoteAddress broker;
    private DefaultMQProducerImpl defaultMQProducer;
    private EventLoopGroup workerGroup;
    private Bootstrap bootstrap;
    private ChannelFuture channelFuture;
    private RemoteHook hook;


    public RemoteClient(DefaultMQProducerImpl impl) {
        this.defaultMQProducer = impl;
        this.broker = this.defaultMQProducer.getAddress();
    }

    @Override
    public void start() {
        String address = broker.getAddress();
        int port = broker.getPort();
        try {
            workerGroup = new NioEventLoopGroup();
            bootstrap = new Bootstrap();
            channelFuture = bootstrap.group(workerGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) throws Exception {
                            ch.pipeline()
                                    .addLast(new NettyEncoder(RemoteCommand.class, new Hessian1Serializer()))
                                    .addLast(new NettyDecoder(RemoteCommand.class, new Hessian1Serializer()))
                                    .addLast(new BaseProducerHandler(hook));


                        }
                    })
                    .connect(address, port)
                    .sync();
            log.info("Finish connect brokcer, ip = {}, port = {}.", address, port);
        } catch (Exception e) {
            log.error("Connect broker error, ", e);
        }
    }
    public void invoke(RemoteCommand command) {
        if (hook != null) {
            log.info("Handel before message hook");
            hook.beforeMessage();
        }
        channelFuture.channel().writeAndFlush(command);
    }
    @Override
    public void shutdown() {
        if (this.workerGroup != null) {
            this.workerGroup.shutdownGracefully();
        }
    }

    @Override
    public void registerHook(RemoteHook hook) {
        this.hook = hook;
    }
}
