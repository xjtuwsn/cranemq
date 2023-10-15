package com.github.xjtuwsn.cranemq.common.remote.enums;

/**
 * @project:cranemq
 * @file:ConnectionEventType
 * @author:wsn
 * @create:2023/10/02-17:22
 */
public enum ConnectionEventType {
    CONNECT,
    IDLE,
    DISCONNECT,
    EXCEPTION,
    PRODUCER_HEARTBEAT,
    CONSUMER_HEARTBEAT,
    BROKER_HEARTBEAT
}
