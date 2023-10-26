package com.github.xjtuwsn.cranemq.broker.store;

import com.github.xjtuwsn.cranemq.common.config.FlushDisk;
import lombok.*;

/**
 * @project:cranemq
 * @file:PersistentConfig
 * @author:wsn
 * @create:2023/10/02-21:15
 */

/**
 * 持久化配置信息
 * @author wsn
 */
@Data
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class PersistentConfig {

    private String cranePath = System.getProperty("user.home") + "\\cranemq\\";
    // 应用存储根目录
    private String rootPath = cranePath + "store\\";

    // 配置信息目录
    private String configPath = cranePath + "config\\";

    // 消费者偏移管理文件
    private String consumerOffsetPath = configPath + "consumerOffset.json";

    // 单个commitLog最大大小
    private int commitLogMaxSize = 1024 * 1024 * 1024; // 1GB

    private int maxLiveTime = 1000 * 60 * 60 * 24 * 3; // 3天

    private String commitLogPath = rootPath + "commitlog\\";

    private String consumerqueuePath = rootPath + "consumequeue\\";

    // 延时日志持久化路径
    private String delayLogPath = rootPath + "delaylog\\";

    private String defaultName = "dummy";

    // 最大堆外借用内存数
    private int maxOutOfMemoryPoolSize = 2;

    // 允许堆外内存
    private boolean enableOutOfMemory = true;

    // 每个索引项长度
    private int queueUnit = 8 + 4;

    // 单个队列索引文件索引数
    private int maxQueueItemNumber = 400000;

    // 单个索引文件大小
    private int maxQueueSize = queueUnit * maxQueueItemNumber;

    // 异步提交间隔
    private long asyncCommitInterval = 1000;

    // 异步刷盘间隔
    private long flushDiskInterval = 500;

    // 刷盘策略，默认异步
    private FlushDisk flushDisk = FlushDisk.ASYNC;

    // 默认主题队列数量
    private int defaultQueueNumber = 4;

    // 单次拉取最大消息数
    private int maxSingleReadLength = 16;

    // 延迟消息日志，100MB
    private int delayMessageLogSize = 100 * 1024 * 1024;


}
