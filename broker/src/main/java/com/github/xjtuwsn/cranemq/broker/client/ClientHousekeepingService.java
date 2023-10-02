package com.github.xjtuwsn.cranemq.broker.client;

import com.github.xjtuwsn.cranemq.broker.BrokerController;
import com.github.xjtuwsn.cranemq.broker.remote.ChannelEventListener;
import io.netty.channel.Channel;

/**
 * @project:cranemq
 * @file:ClientHousekeepingService
 * @author:wsn
 * @create:2023/10/02-19:51
 */
public class ClientHousekeepingService implements ChannelEventListener {
    private BrokerController brokerController;
    public ClientHousekeepingService(BrokerController brokerController) {
        this.brokerController = brokerController;
    }
    @Override
    public void onConnect(Channel channel) {

    }

    @Override
    public void onDisconnect(Channel channel) {

    }

    @Override
    public void onIdle(Channel channel) {

    }

    @Override
    public void onException(Channel channel) {

    }
}
