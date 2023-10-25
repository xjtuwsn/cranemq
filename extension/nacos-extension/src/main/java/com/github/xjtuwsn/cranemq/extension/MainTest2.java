package com.github.xjtuwsn.cranemq.extension;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.Event;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.listener.NamingEvent;
import com.alibaba.nacos.api.naming.pojo.Instance;

import java.util.List;

/**
 * @project:cranemq
 * @file:MainTest2
 * @author:wsn
 * @create:2023/10/25-10:37
 */
public class MainTest2 {

    public static void main(String[] args) throws NacosException {
        NamingService naming = NamingFactory.createNamingService("localhost:8848");

        naming.subscribe("server", new EventListener() {
            @Override
            public void onEvent(Event event) {
                if (event instanceof NamingEvent) {
                    List<Instance> instances =
                            ((NamingEvent) event).getInstances();
                    System.out.println("------------------ " + instances);
                }
            }
        });
        while (true) {

        }
    }
}
