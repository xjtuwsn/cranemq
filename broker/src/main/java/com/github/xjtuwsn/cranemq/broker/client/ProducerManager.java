package com.github.xjtuwsn.cranemq.broker.client;

import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @project:cranemq
 * @file:ProducerManager
 * @author:wsn
 * @create:2023/10/02-16:51
 */
public class ProducerManager {
    private static final Logger log = LoggerFactory.getLogger(ProducerManager.class);

    private ConcurrentHashMap<String, ConcurrentHashMap<Channel, ClientChannelInfo>> producerTable
            = new ConcurrentHashMap<>();

}
