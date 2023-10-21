package com.github.xjtuwsn.cranemq.common.remote.enums;

/**
 * @project:cranemq
 * @file:HandlerType
 * @author:wsn
 * @create:2023/10/02-15:48
 */
public enum HandlerType {
    PRODUCER_REQUEST,
    HEARTBEAT_REQUEST,
    PRODUCER_BATCH_REQUEST,
    CREATE_TOPIC,
    SIMPLE_PULL,
    PULL,
    RECORD_OFFSET,
    QUERY_INFO,
    UPDATE_INFO,
    SEND_BACK
}
