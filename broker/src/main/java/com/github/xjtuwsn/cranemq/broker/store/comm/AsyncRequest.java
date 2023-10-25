package com.github.xjtuwsn.cranemq.broker.store.comm;

import lombok.*;

import java.util.concurrent.CountDownLatch;

/**
 * @project:cranemq
 * @file:AsyncRequest
 * @author:wsn
 * @create:2023/10/04-11:00
 * 进行文件创建所需要的信息
 */
@Data
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class AsyncRequest {
    private StoreRequestType requestType;
    private String fileName;
    private String key;
    private int index;
    private int fileSize;
    private String topic;
    private int queueId;
    private CountDownLatch count;

    public AsyncRequest(int index, String fileName, int fileSize) {
        this.requestType = StoreRequestType.CREATE_MAPPED_FILE;
        this.index = index;
        this.fileName = fileName;
        this.fileSize = fileSize;
    }
    public AsyncRequest(int index, String fileName, int fileSize, String topic, int queueId) {
        this.requestType = StoreRequestType.CREATE_MAPPED_FILE;
        this.index = index;
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.topic = topic;
        this.queueId = queueId;
    }
    public AsyncRequest(StoreRequestType requestType, String fileName, int fileSize) {
        this.requestType = requestType;
        this.fileName = fileName;
        this.fileSize = fileSize;
    }
}
