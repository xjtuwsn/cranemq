package xjtu.wsn.cranemq.common.net.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xjtu.wsn.cranemq.common.net.codec.NettyEncoder;
import xjtu.wsn.cranemq.common.net.serialize.impl.Hessian1Serializer;
import xjtu.wsn.cranemq.common.request.BaseRequest;


/**
 * @project:cranemq
 * @file:ConnectionClient
 * @author:wsn
 * @create:2023/09/26-20:43
 */
public class ConnectionClient implements Client {
    private Logger log= LoggerFactory.getLogger(ConnectionClient.class);
    private RemoteAddress broker;
    private EventLoopGroup workerGroup;
    private Bootstrap bootstrap;
    private ChannelFuture channelFuture;

    public ConnectionClient(RemoteAddress broker) {
        this.broker = broker;
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
                                    .addLast(new LoggingHandler(LogLevel.INFO))
                                    .addLast(new NettyEncoder(BaseRequest.class, new Hessian1Serializer()));

                        }
                    })
                    .connect(address, port)
                    .sync();
            System.out.println("11111111111111111");
            log.info("Finish connect brokcer, ip = {}, port = {}.", address, port);
        } catch (Exception e) {
            log.error("Connect broker error, ", e);
        }
    }

    @Override
    public void close() {
        this.workerGroup.shutdownGracefully();
    }

    @Override
    public void send(BaseRequest request) {
        System.out.println("1111111111111");
        channelFuture.channel().writeAndFlush(request);
    }
}
