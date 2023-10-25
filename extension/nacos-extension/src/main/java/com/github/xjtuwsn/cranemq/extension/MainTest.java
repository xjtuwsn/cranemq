package com.github.xjtuwsn.cranemq.extension;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @project:cranemq
 * @file:MainTest
 * @author:wsn
 * @create:2023/10/24-20:24
 */
public class MainTest {
    public static void main(String[] args) throws NacosException, InterruptedException {
        NamingService naming = NamingFactory.createNamingService("localhost:8848");


        Instance instance1 = new Instance();
        Map<String, String> map1 = new HashMap<>();
        map1.put("1212", "12313");
        instance1.setMetadata(map1);
        instance1.setIp("12.12.12.12");
        instance1.setPort(1212);

        Instance instance2 = new Instance();
        Map<String, String> map2 = new HashMap<>();
        map2.put("topic1", "12313");
        instance2.setMetadata(map2);
        instance2.setIp("12.12.12.12");
        instance2.setPort(1212);
        naming.batchRegisterInstance("server", "12", Arrays.asList(instance1, instance2));
        Thread.sleep(800);
        List<Instance> server = naming.getAllInstances("server");
        System.out.println(server);
        while (true) {

        }
    }
}
