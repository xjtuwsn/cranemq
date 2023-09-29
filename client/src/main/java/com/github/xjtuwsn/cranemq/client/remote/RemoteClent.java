package com.github.xjtuwsn.cranemq.client.remote;

import com.github.xjtuwsn.cranemq.client.hook.RemoteHook;
import com.github.xjtuwsn.cranemq.client.processor.PruducerProcessor;
import com.github.xjtuwsn.cranemq.client.producer.impl.DefaultMQProducerImpl;
import com.github.xjtuwsn.cranemq.client.remote.handler.BaseProducerHandler;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.net.codec.NettyDecoder;
import com.github.xjtuwsn.cranemq.common.net.codec.NettyEncoder;
import com.github.xjtuwsn.cranemq.common.net.serialize.impl.Hessian1Serializer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @project:cranemq
 * @file:RemoteClent
 * @author:wsn
 * @create:2023/09/28-19:21
 */
public class RemoteClent implements RemoteService {
    private static final Logger log = LoggerFactory.getLogger(RemoteClent.class);
    private ConcurrentHashMap<String, ChannelWrapper> channelTable = new ConcurrentHashMap<>();
    private EventLoopGroup workerGroup;
    private Bootstrap bootstrap;
    private RemoteHook hook;
    private DefaultMQProducerImpl defaultMQProducer;

    private ChannelFuture createChannel(String address) {
        ChannelWrapper cw = this.channelTable.get(address);
        if (cw != null && cw.isOk()) {
            return cw.getChannelFuture();
        }
        String[] hostAndPort = address.split(":");
        ChannelFuture cf = this.bootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) throws Exception {
                        ch.pipeline()
                                .addLast(new NettyEncoder(RemoteCommand.class, new Hessian1Serializer()))
                                .addLast(new NettyDecoder(RemoteCommand.class, new Hessian1Serializer()))
                                .addLast(new BaseProducerHandler(hook, new PruducerProcessor(defaultMQProducer)));


                    }
                })
                .connect(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
        log.info("Finish connect brokcer, ip = {}, port = {}.", hostAndPort[0], hostAndPort[1]);
        ChannelWrapper channelWrapper = new ChannelWrapper(cf);
        this.channelTable.put(address, channelWrapper);
        return cf;
    }
    public void invoke(String address, RemoteCommand remoteCommand) {
        ChannelFuture channelFuture = this.createChannel(address);
        if (channelFuture == null || !channelFuture.channel().isActive()) {
            log.error("Create channel error");
            return;
        }
        channelFuture.channel().writeAndFlush(remoteCommand);
    }
    @Override
    public void start() {

    }

    @Override
    public void shutdown() {
        for (ChannelWrapper cw : this.channelTable.values()) {
            if (cw != null) {
                cw.close();
            }
        }
    }

    @Override
    public void registerHook(RemoteHook hook) {
        this.hook = hook;
    }
    static class ChannelWrapper {
        private final ChannelFuture channelFuture;

        public ChannelWrapper(ChannelFuture channelFuture) {
            this.channelFuture = channelFuture;
        }

        public ChannelFuture getChannelFuture() {
            return channelFuture;
        }
        public boolean isOk() {
            return this.channelFuture != null && this.channelFuture.channel().isActive();
        }
        public void close() {
            if (this.channelFuture != null) {
                this.channelFuture.channel().close();
            }
        }
    }
}
