package com.github.xjtuwsn.cranemq.extension.impl;

import com.github.xjtuwsn.cranemq.common.remote.RemoteHook;
import com.github.xjtuwsn.cranemq.common.remote.WritableRegistry;
import com.github.xjtuwsn.cranemq.common.route.QueueData;
import com.github.xjtuwsn.cranemq.extension.AbstractZkRegistry;
import com.github.xjtuwsn.cranemq.extension.ZkNodeData;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @project:cranemq
 * @file:ZkWritableRegistry
 * @author:wsn
 * @create:2023/10/17-16:20
 */
public class ZkWritableRegistry extends AbstractZkRegistry implements WritableRegistry {

    private static final Logger log = LoggerFactory.getLogger(ZkWritableRegistry.class);
    //--------      /cranemq/topic1/brokerName/brokerId --- [address:queueData]
    public ZkWritableRegistry(String address) {
        super(address);
    }


    @Override
    public void uploadRouteInfo(String brokerName, int brokerId, String address, Map<String, QueueData> queueDatas) {
        for (Map.Entry<String, QueueData> entry : queueDatas.entrySet()) {
            String topic = entry.getKey();
            QueueData queueData = entry.getValue();
            ZkNodeData zkNodeData = new ZkNodeData(address, queueData);
            StringBuilder fullPathBulder = new StringBuilder();
            fullPathBulder.append(ROOT).append("/").append(topic).append("/").append(brokerName).append("/")
                    .append(brokerId);
            String fullPath = fullPathBulder.toString();
            byte[] data = dataToByte(zkNodeData);
            try {
                Stat stat = client.checkExists().forPath(fullPath.toString());
                if (stat == null) {
                    client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(fullPath, data);
                } else {
                    client.setData().forPath(fullPath, data);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }
    }

    @Override
    public void append() {

    }
}
