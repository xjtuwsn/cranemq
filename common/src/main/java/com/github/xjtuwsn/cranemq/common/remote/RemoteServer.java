package com.github.xjtuwsn.cranemq.common.remote;

import com.github.xjtuwsn.cranemq.common.remote.enums.HandlerType;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.command.types.RequestType;
import com.github.xjtuwsn.cranemq.common.remote.codec.NettyDecoder;
import com.github.xjtuwsn.cranemq.common.remote.codec.NettyEncoder;
import com.github.xjtuwsn.cranemq.common.remote.event.ChannelEventListener;
import com.github.xjtuwsn.cranemq.common.remote.event.ConnectionEvent;
import com.github.xjtuwsn.cranemq.common.remote.processor.BaseProcessor;
import com.github.xjtuwsn.cranemq.common.remote.serialize.impl.Hessian1Serializer;
import com.github.xjtuwsn.cranemq.common.utils.NetworkUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
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
    private RemoteHook hook;
    private PublishEvent publishEvent;
    private ChannelEventListener channelEventListener;
    private BaseProcessor serverProcessor;

    private Class<? extends ServerChannel> channelClass;
    private ConcurrentHashMap<HandlerType, ExecutorService> threadPoolMap = new ConcurrentHashMap<>();
    public RemoteServer(int listenPort, ChannelEventListener channelEventListener) {
        this.listenPort = listenPort;
        if (useEpoll()) {
            this.bossGroup = new EpollEventLoopGroup(1);
            this.workerGroup = new EpollEventLoopGroup(3);
            this.channelClass = EpollServerSocketChannel.class;
        } else {
            this.bossGroup = new NioEventLoopGroup(1);
            this.workerGroup = new NioEventLoopGroup(3);
            this.channelClass = NioServerSocketChannel.class;
        }
        this.channelEventListener = channelEventListener;
        this.serverBootstrap = new ServerBootstrap();
        this.publishEvent = new PublishEvent();
    }
    public void registerProcessor(BaseProcessor serverProcessor) {
        this.serverProcessor = serverProcessor;
    }
    @Override
    public void start() {
        this.serverBootstrap.group(this.bossGroup, this.workerGroup)
                .channel(this.channelClass)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) throws Exception {
                        ChannelPipeline pipeline = socketChannel.pipeline();
                        pipeline.addLast(new NettyDecoder(RemoteCommand.class, new Hessian1Serializer()))
                                .addLast(new NettyEncoder(RemoteCommand.class, new Hessian1Serializer()))
                                .addLast(new ConnectionManagerHandler(RemoteServer.this))
                                .addLast(new NettyServerHandler());
                    }
                });
        try {
            this.channelFuture = this.serverBootstrap.bind(this.listenPort).sync();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
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


    class NettyServerHandler extends SimpleChannelInboundHandler<RemoteCommand> {
        private final Logger log = LoggerFactory.getLogger(NettyServerHandler.class);
        @Override
        protected void channelRead0(ChannelHandlerContext channelHandlerContext, RemoteCommand request)
                throws Exception {
            if (request == null) {
                log.error("Receive null request from client");
                return;
            }
            RequestType type = (RequestType) request.getHeader().getCommandType();
            switch (type) {
                case MESSAGE_PRODUCE_REQUEST:
                case DELAY_MESSAGE_PRODUCE_REQUEST:
                case MESSAGE_BATCH_PRODUCE_REAUEST:
                    doProducerMessageProcess(channelHandlerContext, request);
                    break;
                case CREATE_TOPIC_REQUEST:
                    doCreateTopicProcess(channelHandlerContext, request);
                    break;
                case HEARTBEAT:
                    doHeartBeatProcess(channelHandlerContext, request);
                    break;
                case SIMPLE_PULL_MESSAGE_REQUEST:
                    doSimplePullProcess(channelHandlerContext, request);
                    break;
                case PULL_MESSAGE:
                    doPullMessageProcess(channelHandlerContext, request);
                    break;
                case QUERY_INFO:
                    doQueryInfoProcess(channelHandlerContext, request);
                    break;
                case RECORD_OFFSET:
                    doRecordOffsetProcess(channelHandlerContext, request);
                    break;
                case LOCK_REQUEST:
                    doLockRequestProcess(channelHandlerContext, request);
                    break;
                case QUERY_TOPIC_REQUEST:
                    doQueryTopicProcess(channelHandlerContext, request);
                    break;
                case UPDATE_TOPIC_REQUEST:
                    doUpdateTopicProcess(channelHandlerContext, request);
                    break;
                case SEND_MESSAGE_BACK:
                    doSendBackProcess(channelHandlerContext, request);
                default:
                    break;
            }
        }
        private void doProducerMessageProcess(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {
            ExecutorService pool = getThreadPool(HandlerType.PRODUCER_REQUEST);
            if (pool != null) {
                pool.execute(() -> {
                    serverProcessor.processProduceMessage(ctx, remoteCommand);
                });
            }
        }
        private void doCreateTopicProcess(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {
            ExecutorService pool = getThreadPool(HandlerType.CREATE_TOPIC);
            if (pool != null) {
                pool.execute(() -> {
                    serverProcessor.processCreateTopic(ctx, remoteCommand);
                });
            }
        }
        private void doHeartBeatProcess(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {
            ExecutorService pool = getThreadPool(HandlerType.HEARTBEAT_REQUEST);
            if (pool != null) {
                pool.execute(() -> {
                    serverProcessor.processHeartBeat(ctx, remoteCommand);
                });
            }
        }

        private void doSimplePullProcess(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {
            ExecutorService pool = getThreadPool(HandlerType.SIMPLE_PULL);
            if (pool != null) {
                pool.execute(() -> {
                    serverProcessor.processSimplePull(ctx, remoteCommand);
                });
            }
        }
        private void doPullMessageProcess(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {
            ExecutorService pool = getThreadPool(HandlerType.PULL);
            if (pool != null) {
                pool.execute(() -> {
                    serverProcessor.processPullRequest(ctx, remoteCommand);
                });
            }
        }

        private void doQueryInfoProcess(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {
            ExecutorService pool = getThreadPool(HandlerType.HEARTBEAT_REQUEST);
            if (pool != null) {
                pool.execute(() -> {
                    serverProcessor.processQueryRequest(ctx, remoteCommand);
                });
            }
        }
        private void doRecordOffsetProcess(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {
            ExecutorService pool = getThreadPool(HandlerType.RECORD_OFFSET);
            if (pool != null) {
                pool.execute(() -> {
                    serverProcessor.processRecordOffsetRequest(ctx, remoteCommand);
                });
            }
        }
        private void doLockRequestProcess(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {
            ExecutorService pool = getThreadPool(HandlerType.SIMPLE_PULL);
            if (pool != null) {
                pool.execute(() -> {
                    serverProcessor.processLockRequest(ctx, remoteCommand);
                });
            }
        }
        private void doQueryTopicProcess(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {
            ExecutorService pool = getThreadPool(HandlerType.QUERY_INFO);
            if (pool != null) {
                pool.execute(() -> {
                    serverProcessor.processQueryRouteRequest(ctx, remoteCommand);
                });
            }
        }

        private void doUpdateTopicProcess(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {
            ExecutorService pool = getThreadPool(HandlerType.UPDATE_INFO);
            if (pool != null) {
                pool.execute(() -> {
                    serverProcessor.processUpdateRequest(ctx, remoteCommand);
                });
            }
        }

        private void doSendBackProcess(ChannelHandlerContext ctx, RemoteCommand remoteCommand) {
            ExecutorService pool = getThreadPool(HandlerType.SEND_BACK);
            if (pool != null) {
                pool.execute(() -> {
                    serverProcessor.processSendBackRequest(ctx, remoteCommand);
                });
            }
        }
    }

    // TODO 连接与断联、心跳等发布事件
    class PublishEvent extends Thread {
        private final Logger log = LoggerFactory.getLogger(PublishEvent.class);
        private LinkedBlockingQueue<ConnectionEvent> queue = new LinkedBlockingQueue<>();
        public void publish(ConnectionEvent event) {
            int size = queue.size();
            if (size > 20000) {
                log.warn("Too much event, connote been handled, number is {}", size);
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
                                break;
                            case BROKER_HEARTBEAT:
                                RemoteServer.this.channelEventListener.onBrokerHeartBeat(channel, event.getBrokerName(),
                                        event.getBrokerId());
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
