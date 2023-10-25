package com.github.xjtuwsn.cranemq.extension;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.github.xjtuwsn.cranemq.common.remote.RemoteHook;
import com.github.xjtuwsn.cranemq.common.remote.RemoteService;
import com.github.xjtuwsn.cranemq.common.route.TopicRouteInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @project:cranemq
 * @file:AbstractNacosRegistry
 * @author:wsn
 * @create:2023/10/25-10:45
 */
public class AbstractNacosRegistry implements RemoteService {

    private static final Logger log = LoggerFactory.getLogger(AbstractNacosRegistry.class);

    protected static final String GROUP_NAME = "cranemq";
    protected static final String BROKER_NAME = "brokerName";

    protected static final String BROKER_ID = "brokerId";
    protected static final String ADDRESS = "address";

    protected static final String QUEUE_NUMBER = "queueNumber";

    protected NamingService namingService;

    private String address;

    public AbstractNacosRegistry(String address) {
        this.address = address;
    }
    protected TopicRouteInfo byteToData(byte[] bytes) {
        String str = new String(bytes);
        return JSONObject.parseObject(str, TopicRouteInfo.class);
    }

    protected byte[] dataToByte(TopicRouteInfo data) {
        String jsonString = JSON.toJSONString(data);
        return jsonString.getBytes();
    }
    @Override
    public void start() {
        try {
            this.namingService = NamingFactory.createNamingService(address);
        } catch (NacosException e) {
            log.error("Connect nacos server error");
            throw new RuntimeException(e);
        }
    }

    @Override
    public void shutdown() {
        try {
            this.namingService.shutDown();
        } catch (NacosException e) {
            log.error("Close nacos remote server error");
            throw new RuntimeException(e);
        }
    }

    @Override
    public void registerHook(RemoteHook hook) {

    }
}
