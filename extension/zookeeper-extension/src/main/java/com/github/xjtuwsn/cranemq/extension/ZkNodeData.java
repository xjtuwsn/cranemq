package com.github.xjtuwsn.cranemq.extension;


import com.github.xjtuwsn.cranemq.common.route.QueueData;

/**
 * @project:cranemq
 * @file:ZkNodeData
 * @author:wsn
 * @create:2023/10/17-16:42
 */
public class ZkNodeData {

    private String address;
    private QueueData queueData;

    public ZkNodeData() {
    }

    public ZkNodeData(String address, QueueData queueData) {
        this.address = address;
        this.queueData = queueData;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public QueueData getQueueData() {
        return queueData;
    }

    public void setQueueData(QueueData queueData) {
        this.queueData = queueData;
    }
}
