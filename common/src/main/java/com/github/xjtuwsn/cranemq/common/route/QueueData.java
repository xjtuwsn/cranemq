package com.github.xjtuwsn.cranemq.common.route;

/**
 * @project:cranemq
 * @file:QueueData
 * @author:wsn
 * @create:2023/09/28-20:09
 */
public class QueueData {
    private String brokerName;
    private int readQueueNums;
    private int writeQueueNums;

    public QueueData() {
    }

    public QueueData(String brokerName) {
        this.brokerName = brokerName;
    }

    public QueueData(String brokerName, int readQueueNums, int writeQueueNums) {
        this.brokerName = brokerName;
        this.readQueueNums = readQueueNums;
        this.writeQueueNums = writeQueueNums;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public int getReadQueueNums() {
        return readQueueNums;
    }

    public int getWriteQueueNums() {
        return writeQueueNums;
    }
}
