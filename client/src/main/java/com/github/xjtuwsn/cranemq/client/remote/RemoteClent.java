package com.github.xjtuwsn.cranemq.client.remote;

import com.github.xjtuwsn.cranemq.common.net.RemoteHook;
import com.github.xjtuwsn.cranemq.client.processor.PruducerProcessor;
import com.github.xjtuwsn.cranemq.client.producer.impl.DefaultMQProducerImpl;
import com.github.xjtuwsn.cranemq.client.remote.handler.BaseProducerHandler;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.exception.CraneClientException;
import com.github.xjtuwsn.cranemq.common.net.RemoteService;
import com.github.xjtuwsn.cranemq.common.net.codec.NettyDecoder;
import com.github.xjtuwsn.cranemq.common.net.codec.NettyEncoder;
import com.github.xjtuwsn.cranemq.common.net.serialize.impl.Hessian1Serializer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
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

    public RemoteClent(DefaultMQProducerImpl impl) {
        this.defaultMQProducer = impl;
    }

    private ChannelFuture createChannel(String address) {
        ChannelWrapper cw = this.channelTable.get(address);
        if (cw != null && cw.isOk()) {
            return cw.getChannelFuture();
        }
        String[] hostAndPort = address.split(":");
        try {
            Bootstrap bootstrap1 = new Bootstrap();
            ChannelFuture cf = bootstrap1.group(workerGroup)
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
                    .connect(hostAndPort[0], Integer.parseInt(hostAndPort[1])).sync();
            log.info("Finish connect brokcer, ip = {}, port = {}.", hostAndPort[0], hostAndPort[1]);
            ChannelWrapper channelWrapper = new ChannelWrapper(cf);
            this.channelTable.put(address, channelWrapper);
            return cf;
        } catch (InterruptedException e) {
            throw new CraneClientException("Creat channel error");
        }

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
        this.workerGroup = new NioEventLoopGroup(1);
        this.bootstrap = new Bootstrap();
    }

    @Override
    public void shutdown() {
        for (ChannelWrapper cw : this.channelTable.values()) {
            if (cw != null) {
                cw.close();
            }
        }
        this.workerGroup.shutdownGracefully();
    }
    public void markExpired(List<String> addresses) {
        if (addresses == null || addresses.size() == 0) return;
        for (String address : addresses) {
            ChannelWrapper cw = this.channelTable.get(address);
            if (cw != null) {
                cw.setExpired();
            }
        }
    }
    public void cleanExpired() {
        Iterator<String> iterator = this.channelTable.keySet().iterator();
        while (iterator.hasNext()) {
            String key = iterator.next();
            ChannelWrapper cw = this.channelTable.get(key);
            if (cw.isExpired()) {
                log.info("FutureChannel {} has expired, and will be cleaned", cw);
                cw.close();
                iterator.remove();
            }
        }
    }
    public ConcurrentHashMap<String, ChannelWrapper> getChannelTable() {
        return channelTable;
    }

    @Override
    public void registerHook(RemoteHook hook) {
        this.hook = hook;
    }
    static class ChannelWrapper {
        private final ChannelFuture channelFuture;
        private boolean expired;

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

        public boolean isExpired() {
            return expired;
        }

        public void setExpired() {
            this.expired = true;
        }
    }
}
