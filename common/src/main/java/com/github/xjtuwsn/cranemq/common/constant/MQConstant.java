package com.github.xjtuwsn.cranemq.common.constant;

/**
 * @project:cranemq
 * @file:ProducerConstant
 * @author:wsn
 * @create:2023/09/27-11:18
 */
public class MQConstant {
    public static final String DEFAULT_TOPIC_NAME = "default-topic";
    public static final String DEFAULT_GROUP_NAME = "default-group";
    public static final long RESPONSE_TIMEOUT_MILLS = 115000;

    public static final int MAX_RETRY_TIMES = 2;

    public static final int DEFAULT_QUEUE_NUMBER = 4;
    public static final int MASTER_ID = 0;

    public static final String DEFAULT_CONF_PATH = "D:\\code\\opensource\\cranemq\\default.conf";
    public static final String CHARSETNAME = "UTF-8";
    public static final int COMMITLOG_FILENAME_LENGTH = 20;
}
