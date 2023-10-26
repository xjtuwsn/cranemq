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
    public static final String DEFAULT_CONSUMER_GROUP = "default-consumer";

    public static final String DELAY_TOPIC_NAME = "DELAY_TOPIC";

    public static final String RETRY_PREFIX = "RETRY_TOPIC-";

    public static final String DLQ_PREFIX = "DLQ_TOPIC-";

    public static final String GRAY_SUFFIX = "%%GRAY";
    public static final long RESPONSE_TIMEOUT_MILLS = 3000;
    public static final long MAX_PULL_TIMEOUT_MILLS = 3000;

    public static final int MAX_RETRY_TIMES = 2;

    public static final int DEFAULT_QUEUE_NUMBER = 4;
    public static final int MASTER_ID = 0;

    public static final String CLASSPATH_SUFFIX = "classpath:";

    public static final String DEFAULT_LOCAL_OFFSET_PATH = System.getProperty("user.home") +  "\\cranemq\\";
    public static final String LOCAL_OFFSET_SUFFIX = "_offset.json";
    public static final String CHARSETNAME = "UTF-8";
    public static final int COMMITLOG_FILENAME_LENGTH = 20;
}
