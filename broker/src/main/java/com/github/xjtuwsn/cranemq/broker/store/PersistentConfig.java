package com.github.xjtuwsn.cranemq.broker.store;

import lombok.*;

/**
 * @project:cranemq
 * @file:PersistentConfig
 * @author:wsn
 * @create:2023/10/02-21:15
 */
@Data
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class PersistentConfig {

    private String rootPath = "D:\\cranemq\\store\\";
    private int commitLogMaxSize = 1024 * 1024 * 1024; // 1GB
    private int maxLiveTime = 1000 * 60 * 60 * 24 * 3; // 3天

    private String commitLogPath = rootPath + "commitlog\\";
    private String consumerqueuePath = rootPath + "consumequeue\\";
    private String defaultName = "dummy";
    private int maxOutOfMemoryPoolSize = 3;
    private boolean enableOutOfMemory = true;

    private int queueUnit = 8 + 4;
    private int maxQueueItemNumber = 400000;
    private int maxQueueSize = queueUnit * maxQueueItemNumber;

    private long asyncCommitInterval = 3000;
    private long flushDiskInterval = 1000;

}
