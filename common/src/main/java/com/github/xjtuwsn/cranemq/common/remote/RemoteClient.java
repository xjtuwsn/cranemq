package com.github.xjtuwsn.cranemq.common.remote;

import com.github.xjtuwsn.cranemq.common.command.types.ResponseType;
import com.github.xjtuwsn.cranemq.common.command.types.Type;
import com.github.xjtuwsn.cranemq.common.entity.ClientType;
import com.github.xjtuwsn.cranemq.common.command.RemoteCommand;
import com.github.xjtuwsn.cranemq.common.exception.CraneClientException;
import com.github.xjtuwsn.cranemq.common.remote.codec.NettyDecoder;
import com.github.xjtuwsn.cranemq.common.remote.codec.NettyEncoder;
import com.github.xjtuwsn.cranemq.common.remote.processor.BaseProcessor;
import com.github.xjtuwsn.cranemq.common.remote.serialize.impl.Hessian1Serializer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @project:cranemq
 * @file:RemoteClent
 * @author:wsn
 * @create:2023/09/28-19:21
 */
public class RemoteClient implements RemoteService {
    private static final Logger log = LoggerFactory.getLogger(RemoteClient.class);
    private ConcurrentHashMap<String, ChannelWrapper> channelTable = new ConcurrentHashMap<>();
    private EventLoopGroup workerGroup;
    private Bootstrap bootstrap;
    private RemoteHook hook;
    private ConcurrentHashMap<ClientType, BaseProcessor> processorTable = new ConcurrentHashMap<>();
    private int coreSize = 8;
    private int maxSize = 16;
    private ExecutorService asyncCallBackService;

    public RemoteClient() {
        this.asyncCallBackService = new ThreadPoolExecutor(coreSize,
                maxSize,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(1000),
                new ThreadFactory() {
                    AtomicInteger count = new AtomicInteger(0);
                    @Override
                    public Thread newThread(Runnable r) {
                        int andIncrement = count.getAndIncrement();
                        return new Thread(r, "CallBack handler thread, no." + andIncrement);
                    }
                },
                new ThreadPoolExecutor.AbortPolicy());
    }
    public void registerProcessor(ClientType type, BaseProcessor processor) {
        this.processorTable.put(type, processor);
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
                                    .addLast(new NettyClientHandler());
                        }
                    })
                    .connect(hostAndPort[0], Integer.parseInt(hostAndPort[1])).sync();
            log.info("Finish connect remote, ip = {}, port = {}.", hostAndPort[0], hostAndPort[1]);
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
        ChannelFuture channelFuture1 = channelFuture.channel().writeAndFlush(remoteCommand);

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
        if (addresses == null || addresses.size() == 0) {
            return;
        }
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

    class NettyClientHandler extends SimpleChannelInboundHandler<RemoteCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext channelHandlerContext, RemoteCommand remoteCommand) throws Exception {
            if (remoteCommand == null) {
                log.error("Receive null message from broker");
                return;
            }
            Type type = remoteCommand.getHeader().getCommandType();
            if (!(type instanceof ResponseType)) {
                log.error("Receive wrong type response, {}", type);
                return;
            }
            switch ((ResponseType) type) {
                case PRODUCE_MESSAGE_RESPONSE:
                    doProduceProcessor(remoteCommand);
                    break;
                case QUERY_BROKER_RESPONSE:
                    doQueryProcessor(remoteCommand);
                    break;
                case QUERY_TOPIC_RESPONSE:
                    doUpdateTopicProcessor(remoteCommand);
                    break;
                case CREATE_TOPIC_RESPONSE:
                    doCreateTopicProcessor(remoteCommand);
                    break;
                case SIMPLE_PULL_RESPONSE:
                    doSimplePullProcessor(remoteCommand);
                    break;
                case NOTIFY_CHAGED_RESPONSE:
                    doNotifyChangedProcessor(remoteCommand);
                    break;
                case PULL_RESPONSE:
                    doPullResponseProcessor(remoteCommand);
                    break;
                case LOCK_RESPONSE:
                    doLockResponse(remoteCommand);
                    break;
                default:
                    break;
            }
        }
        private void doProduceProcessor(RemoteCommand remoteCommand) {
            processorTable.get(ClientType.PRODUCER).processMessageProduceResopnse(remoteCommand, asyncCallBackService, hook);
        }
        private void doUpdateTopicProcessor(RemoteCommand remoteCommand) {
            processorTable.get(ClientType.BOTH).processUpdateTopicResponse(remoteCommand, asyncCallBackService);
        }
        private void doCreateTopicProcessor(RemoteCommand remoteCommand) {
            processorTable.get(ClientType.PRODUCER).processCreateTopicResponse(remoteCommand, asyncCallBackService);
        }
        private void doQueryBrokerProcessor(RemoteCommand remoteCommand) {

        }
        private void doSimplePullProcessor(RemoteCommand remoteCommand) {
            processorTable.get(ClientType.CONSUMER).processSimplePullResponse(remoteCommand, asyncCallBackService, hook);
        }
        private void doNotifyChangedProcessor(RemoteCommand remoteCommand) {
            processorTable.get(ClientType.CONSUMER).processNotifyChangedResponse(remoteCommand, asyncCallBackService);
        }
        private void doPullResponseProcessor(RemoteCommand remoteCommand) {
            processorTable.get(ClientType.CONSUMER).processPullResponse(remoteCommand, asyncCallBackService);
        }
        private void doQueryProcessor(RemoteCommand remoteCommand) {
            processorTable.get(ClientType.CONSUMER).processQueryResponse(remoteCommand, asyncCallBackService);
        }
        private void doLockResponse(RemoteCommand remoteCommand) {
            processorTable.get(ClientType.CONSUMER).processLockResponse(remoteCommand, asyncCallBackService);
        }
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
