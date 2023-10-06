package com.github.xjtuwsn.cranemq.broker.remote;

import com.github.xjtuwsn.cranemq.broker.BrokerController;
import com.github.xjtuwsn.cranemq.broker.enums.HandlerType;
import com.github.xjtuwsn.cranemq.broker.handler.BaseHandler;
import com.github.xjtuwsn.cranemq.broker.handler.ConnectionManagerHandler;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.net.RemoteHook;
import com.github.xjtuwsn.cranemq.common.net.RemoteService;
import com.github.xjtuwsn.cranemq.common.net.codec.NettyDecoder;
import com.github.xjtuwsn.cranemq.common.net.codec.NettyEncoder;
import com.github.xjtuwsn.cranemq.common.net.serialize.impl.Hessian1Serializer;
import com.github.xjtuwsn.cranemq.common.utils.NetworkUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @project:cranemq
 * @file:RemoteServer
 * @author:wsn
 * @create:2023/10/02-14:41
 */
public class RemoteServer implements RemoteService {
    private int listenPort;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ServerBootstrap serverBootstrap;
    private ChannelFuture channelFuture;
    private BrokerController controller;
    private RemoteHook hook;
    private PublishEvent publishEvent;
    private ChannelEventListener channelEventListener;
    private ConcurrentHashMap<HandlerType, ExecutorService> threadPoolMap = new ConcurrentHashMap<>();
    public RemoteServer(int listenPort, BrokerController controller, ChannelEventListener channelEventListener) {
        this.controller = controller;
        this.listenPort = listenPort;
        if (useEpoll()) {
            this.bossGroup = new EpollEventLoopGroup(1);
            this.workerGroup = new EpollEventLoopGroup(3);
        } else {
            this.bossGroup = new NioEventLoopGroup(1);
            this.workerGroup = new NioEventLoopGroup(3);
        }
        this.channelEventListener = channelEventListener;
        this.serverBootstrap = new ServerBootstrap();
        this.publishEvent = new PublishEvent();
    }
    @Override
    public void start() {
        this.serverBootstrap.group(this.bossGroup, this.workerGroup)
                .channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) throws Exception {
                        ChannelPipeline pipeline = socketChannel.pipeline();
                        pipeline.addLast(new NettyDecoder(RemoteCommand.class, new Hessian1Serializer()))
                                .addLast(new NettyEncoder(RemoteCommand.class, new Hessian1Serializer()))
                                .addLast(new ConnectionManagerHandler(RemoteServer.this))
                                .addLast(new BaseHandler(RemoteServer.this));
                    }
                });
        this.channelFuture = this.serverBootstrap.bind(this.listenPort);
        this.publishEvent.start();
    }
    private boolean useEpoll() {
        return NetworkUtil.isLinuxPlatform() && Epoll.isAvailable();
    }
    public void registerThreadPool(HandlerType type, ExecutorService executorService) {
        this.threadPoolMap.put(type, executorService);
    }

    public void publishEvent(ConnectionEvent event) {
        this.publishEvent.publish(event);
    }
    public boolean hasListener() {
        return this.channelEventListener != null;
    }
    public ExecutorService getThreadPool(HandlerType type) {
        return this.threadPoolMap.get(type);
    }
    @Override
    public void shutdown() {
        if (this.workerGroup != null) {
            this.workerGroup.shutdownGracefully();
        }
        if (this.bossGroup != null) {
            this.bossGroup.shutdownGracefully();
        }
        if (this.channelFuture != null) {
            this.channelFuture.channel().close();
        }
    }

    @Override
    public void registerHook(RemoteHook hook) {
        this.hook = hook;
    }

    public BrokerController getController() {
        return controller;
    }

    // TODO 连接与断联、心跳等发布事件
    class PublishEvent extends Thread {
        private final Logger log = LoggerFactory.getLogger(PublishEvent.class);
        private LinkedBlockingQueue<ConnectionEvent> queue = new LinkedBlockingQueue<>();
        public void publish(ConnectionEvent event) {
            int size = queue.size();
            if (size > 20000) {
                log.warn("Too much event, connot been handled, number is {}", size);
            } else {
                queue.add(event);
            }
        }
        @Override
        public void run() {
            while (true) {
                try {
                    ConnectionEvent event = queue.poll(3000, TimeUnit.MILLISECONDS);
                    if (event != null) {
                        Channel channel = event.getChannel();
                        switch (event.getEventType()) {
                            case CONNECT:
                                RemoteServer.this.channelEventListener.onConnect(channel);
                                break;
                            case DISCONNECT:
                                RemoteServer.this.channelEventListener.onDisconnect(channel);
                                break;
                            case IDLE:
                                RemoteServer.this.channelEventListener.onIdle(channel);
                                break;
                            case EXCEPTION:
                                RemoteServer.this.channelEventListener.onException(channel);
                                break;
                            case PRODUCER_HEARTBEAT:
                                RemoteServer.this.channelEventListener.onProducerHeartBeat(event.getHeartBeatRequest(),
                                        channel);
                                break;
                            case CONSUMER_HEARTBEAT:
                                RemoteServer.this.channelEventListener.onConsumerHeartBeat(event.getHeartBeatRequest(),
                                        channel);
                            default:
                                break;
                        }
                    }
                } catch (InterruptedException e) {
                    log.warn("EventPublisher has been interrupted");
                }

            }
        }
    }
}
